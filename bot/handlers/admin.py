"""Admin command handlers."""
from __future__ import annotations

import asyncio
import logging

from telegram import Update
from telegram.ext import ContextTypes

from bot.db import crud
from bot.db.database import get_sessionmaker
from bot.handlers.media import hash_cache, is_caller_admin

logger = logging.getLogger(__name__)

ADMIN_ONLY = "⛔ This command is for group administrators only."

_pending_clear: dict[int, asyncio.Event] = {}


async def _reply(update: Update, text: str) -> None:
    if update.effective_message:
        await update.effective_message.reply_text(text)


async def cmd_dedup_on(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await is_caller_admin(update, context):
        return await _reply(update, ADMIN_ONLY)
    factory = get_sessionmaker()
    async with factory() as session:
        await crud.update_chat_settings(session, update.effective_chat.id, enabled=True)
        await session.commit()
    logger.info("dedup_on: chat=%s admin=%s", update.effective_chat.id, update.effective_message.from_user.id)
    await _reply(update, "✅ Duplicate detection *enabled* for this chat.")


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
    _pending_clear[chat_id] = event
    await _reply(
        update,
        "⚠️ Reply to this message with `CONFIRM` within 60 seconds "
        "to delete ALL stored hashes for this chat.",
    )

    try:
        await asyncio.wait_for(event.wait(), timeout=60.0)
    except asyncio.TimeoutError:
        _pending_clear.pop(chat_id, None)
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
    event = _pending_clear.get(chat_id)
    if event is None:
        return
    if not await is_caller_admin(update, context):
        return
    _pending_clear.pop(chat_id, None)
    event.set()


async def cmd_dedup_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await is_caller_admin(update, context):
        return await _reply(update, ADMIN_ONLY)
    chat_id = update.effective_chat.id
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
