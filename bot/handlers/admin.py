"""Admin command handlers."""
from __future__ import annotations

import asyncio
import logging

from telegram import Update
from telegram.error import BadRequest, Forbidden, TelegramError
from telegram.ext import ContextTypes
from sqlalchemy import select

from bot.db import crud
from bot.db.database import get_sessionmaker
from bot.db.models import DuplicateLog
from bot.handlers.media import hash_cache, is_caller_admin

logger = logging.getLogger(__name__)

ADMIN_ONLY = "⛔ This command is for group administrators only."

_PENDING_CLEAR: dict[int, asyncio.Event] = {}

COMMAND_HELP = (
    "🤖 **Telegram Dedup Bot**\n\n"
    "I detect and delete duplicate media in groups.\n\n"
    "**How it works:**\n"
    "1. Run `/dedup_on` to enable detection\n"
    "2. Send any media — I'll hash it and store it\n"
    "3. Send the same media again — I'll auto-delete it\n\n"
    "**Commands:**\n"
    "/dedup_on — Enable duplicate detection\n"
    "/dedup_off — Disable duplicate detection\n"
    "/set_threshold — Set image/video similarity threshold\n"
    "/dedup_status — Show current settings\n"
    "/dedup_stats — Show duplicate statistics\n"
    "/clean — Delete all tracked duplicates from the chat\n"
    "/clear_hashes — Wipe all stored hashes (requires CONFIRM)\n"
    "/help — Show this message"
)

COMMAND_DESCRIPTIONS = [
    ("start", "Show welcome message and command list"),
    ("dedup_on", "Enable duplicate detection for this chat"),
    ("dedup_off", "Disable duplicate detection for this chat"),
    ("set_threshold", "Set similarity threshold for image or video"),
    ("dedup_status", "Show enabled/disabled state, thresholds, and stats"),
    ("dedup_stats", "Show global + per-chat duplicate statistics"),
    ("clean", "Delete all tracked duplicate messages from this chat"),
    ("clear_hashes", "Wipe stored hashes (requires CONFIRM reply)"),
    ("help", "Show command reference"),
]


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _reply(update, COMMAND_HELP)


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _reply(update, COMMAND_HELP)


async def _reply(update: Update, text: str) -> None:
    if update.effective_message:
        await update.effective_message.reply_text(text)


async def cmd_dedup_on(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    if not await is_caller_admin(update, context):
        return await _reply(update, ADMIN_ONLY)
    try:
        factory = get_sessionmaker()
        async with factory() as session:
            await crud.update_chat_settings(session, chat_id, enabled=True)
            await session.commit()
        logger.info("dedup_on: chat=%s admin=%s", chat_id, update.effective_message.from_user.id)
        await _reply(update, "✅ Duplicate detection *enabled*. Send any media — duplicates will be auto-deleted. Use /clean to delete already-tracked duplicates.")
    except Exception as exc:
        logger.exception("dedup_on failed for chat %s: %s", chat_id, exc)
        await _reply(update, f"❌ Error enabling detection: {exc}")


async def cmd_dedup_off(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await is_caller_admin(update, context):
        return await _reply(update, ADMIN_ONLY)
    factory = get_sessionmaker()
    async with factory() as session:
        await crud.update_chat_settings(session, update.effective_chat.id, enabled=False)
        await session.commit()
    logger.info("dedup_off: chat=%s admin=%s", update.effective_chat.id, update.effective_message.from_user.id)
    await _reply(update, "🛑 Duplicate detection *disabled* for this chat.")


async def cmd_set_threshold(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await is_caller_admin(update, context):
        return await _reply(update, ADMIN_ONLY)
    if not context.args or len(context.args) < 2:
        return await _reply(update, "Usage: /set_threshold <image|video> <0-20>")
    target = context.args[0].lower()
    if target not in ("image", "video"):
        return await _reply(update, "Target must be *image* or *video*.")
    try:
        value = int(context.args[1])
    except ValueError:
        return await _reply(update, "Threshold must be an integer in 0..20")
    if not 0 <= value <= 20:
        return await _reply(update, "Threshold must be between 0 and 20.")
    factory = get_sessionmaker()
    chat_id = update.effective_chat.id
    async with factory() as session:
        if target == "image":
            await crud.update_chat_settings(session, chat_id, threshold=value)
        else:
            await crud.update_chat_settings(session, chat_id, video_threshold=value)
        await session.commit()
    label = "Image" if target == "image" else "Video"
    logger.info("set_threshold: chat=%s admin=%s target=%s value=%s", chat_id, update.effective_message.from_user.id, target, value)
    await _reply(update, f"📐 {label} similarity threshold set to {value}.")


async def cmd_dedup_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await is_caller_admin(update, context):
        return await _reply(update, ADMIN_ONLY)
    chat_id = update.effective_chat.id
    factory = get_sessionmaker()
    async with factory() as session:
        s = await crud.get_chat_settings(session, chat_id)
        count = await crud.count_hashes_for_chat(session, chat_id)
    await _reply(
        update,
        f"📊 Status:\n"
        f"  • Enabled: {s.enabled}\n"
        f"  • Image threshold: {s.threshold}\n"
        f"  • Video threshold: {s.video_threshold}\n"
        f"  • Stored hashes: {count}\n"
        f"  • Duplicates deleted: {s.duplicates_deleted}",
    )
    logger.info("dedup_status: chat=%s admin=%s", chat_id, update.effective_message.from_user.id)


async def cmd_clear_hashes(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await is_caller_admin(update, context):
        return await _reply(update, ADMIN_ONLY)
    chat_id = update.effective_chat.id

    event = asyncio.Event()
    _PENDING_CLEAR[chat_id] = event
    await _reply(
        update,
        "⚠️ Reply to this message with `CONFIRM` within 60 seconds "
        "to delete ALL stored hashes for this chat.",
    )

    try:
        await asyncio.wait_for(event.wait(), timeout=60.0)
    except asyncio.TimeoutError:
        _PENDING_CLEAR.pop(chat_id, None)
        return await _reply(update, "⌛ Confirmation timed out. No hashes were deleted.")

    factory = get_sessionmaker()
    async with factory() as session:
        deleted = await crud.delete_hashes_for_chat(session, chat_id)
        await session.commit()
    hash_cache.invalidate(chat_id)
    logger.info("clear_hashes: chat=%s admin=%s deleted=%s", chat_id, update.effective_message.from_user.id, deleted)
    await _reply(update, f"🧹 Deleted {deleted} stored hashes for this chat.")


async def cmd_confirm_clear(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.effective_message
    if msg is None or msg.text is None:
        return
    if msg.text.strip() != "CONFIRM":
        return
    chat_id = update.effective_chat.id
    event = _PENDING_CLEAR.get(chat_id)
    if event is None:
        return
    if not await is_caller_admin(update, context):
        return
    _PENDING_CLEAR.pop(chat_id, None)
    event.set()


async def cmd_dedup_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await is_caller_admin(update, context):
        return await _reply(update, ADMIN_ONLY)
    chat_id = update.effective_chat.id
    try:
        factory = get_sessionmaker()
        async with factory() as session:
            global_ = await crud.global_stats(session)
            breakdown = await crud.per_chat_breakdown(session)
            top_senders = await crud.top_duplicate_senders(session, chat_id, limit=5)
            by_type = await crud.duplicates_by_media_type(session, chat_id)
        lines = [
            "📈 Global stats:",
            f"  • Total unique media: {global_['total_unique_media']}",
            f"  • Total duplicates deleted: {global_['total_duplicates_deleted']}",
            "",
            "Per-chat breakdown (chat_id, unique, deleted):",
        ]
        for cid, unique, deleted in breakdown:
            lines.append(f"  • {cid}: {unique} unique, {deleted} deleted")
        if top_senders:
            lines.append("")
            lines.append("Top 5 duplicate senders in this chat:")
            for sender_id, count in top_senders:
                lines.append(f"  • User {sender_id}: {count} duplicates")
        if by_type:
            lines.append("")
            lines.append("Duplicates by media type:")
            for mtype, count in by_type:
                lines.append(f"  • {mtype or 'unknown'}: {count}")
        logger.info("dedup_stats: chat=%s admin=%s", chat_id, update.effective_message.from_user.id)
        await _reply(update, "\n".join(lines))
    except Exception as exc:
        logger.exception("dedup_stats failed for chat %s: %s", chat_id, exc)
        await _reply(update, f"❌ Error fetching stats: {exc}")


async def cmd_clean(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Delete all tracked duplicate messages for this chat."""
    if not await is_caller_admin(update, context):
        return await _reply(update, ADMIN_ONLY)
    chat_id = update.effective_chat.id
    try:
        factory = get_sessionmaker()
        async with factory() as session:
            stmt = (
                select(DuplicateLog)
                .where(
                    DuplicateLog.chat_id == chat_id,
                    DuplicateLog.action_taken == "DELETED",
                )
            )
            result = await session.execute(stmt)
            logs = list(result.scalars().all())
            deleted_count = 0
            for log_entry in logs:
                try:
                    await context.bot.delete_message(
                        chat_id=chat_id, message_id=log_entry.duplicate_message_id
                    )
                    deleted_count += 1
                    await asyncio.sleep(0.5)
                except (Forbidden, BadRequest):
                    pass
                except TelegramError:
                    pass
        logger.info("clean: chat=%s admin=%s deleted=%d", chat_id, update.effective_message.from_user.id, deleted_count)
        await _reply(update, f"🧹 Cleaned {deleted_count} tracked duplicate messages.")
    except Exception as exc:
        logger.exception("clean failed for chat %s: %s", chat_id, exc)
        await _reply(update, f"❌ Error during clean: {exc}")
