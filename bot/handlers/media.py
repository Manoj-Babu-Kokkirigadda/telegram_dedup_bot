"""Telegram media-message handler."""
from __future__ import annotations

import asyncio
import logging
import os
import time
import uuid
from collections import defaultdict, deque

from telegram import Update
from telegram.constants import ChatMemberStatus
from telegram.error import BadRequest, Forbidden, TelegramError
from telegram.ext import ContextTypes

from bot.core.cache import HashCache
from bot.core.detector import process_media
from bot.db import crud
from bot.db.database import get_sessionmaker

logger = logging.getLogger(__name__)

hash_cache: HashCache = HashCache()
download_dir: str = "./downloads"

_rate_window_seconds = 60
_rate_max_messages = 50
_rate_pause_seconds = 30
_rate_state: dict[int, deque[float]] = defaultdict(deque)
_rate_paused: dict[int, float] = {}

_user_rate_window = 60
_user_rate_max = 20
_user_rate_state: dict[int, deque[float]] = defaultdict(deque)
_user_spam_notified: set[int] = set()

_perm_warning_state: dict[int, float] = {}
_perm_warning_interval = 3600


notify_on_delete: bool = True
_notify_cooldown: dict[int, float] = {}
_NOTIFY_COOLDOWN = 30

TG_MAX_FILE_SIZE = 20 * 1024 * 1024  # 20MB Bot API limit


def configure(
    *,
    cache: HashCache,
    downloads: str,
    max_media_per_min: int = 50,
    rate_pause_seconds: int = 30,
    max_user_media_per_min: int = 20,
    send_delete_notification: bool = True,
) -> None:
    global hash_cache, download_dir, _rate_max_messages, _rate_pause_seconds
    global _user_rate_max, notify_on_delete
    hash_cache = cache
    download_dir = downloads
    _rate_max_messages = max_media_per_min
    _rate_pause_seconds = rate_pause_seconds
    _user_rate_max = max_user_media_per_min
    notify_on_delete = send_delete_notification
    os.makedirs(download_dir, exist_ok=True)


def _rate_limited(chat_id: int) -> bool:
    now = time.monotonic()
    paused_until = _rate_paused.get(chat_id, 0.0)
    if now < paused_until:
        return True

    dq = _rate_state[chat_id]
    while dq and now - dq[0] > _rate_window_seconds:
        dq.popleft()
    if len(dq) >= _rate_max_messages:
        _rate_paused[chat_id] = now + _rate_pause_seconds
        logger.warning(
            "Rate limit hit for chat %s — pausing dedup for %ds",
            chat_id, _rate_pause_seconds,
        )
        return True
    dq.append(now)
    return False


def _user_rate_check(user_id: int) -> bool:
    """Return True if user has exceeded per-user rate (spam suspect)."""
    now = time.monotonic()
    dq = _user_rate_state[user_id]
    while dq and now - dq[0] > _user_rate_window:
        dq.popleft()
    dq.append(now)
    return len(dq) > _user_rate_max


def _classify(message) -> tuple[str, object] | None:
    """Return (file_type, telegram_file_obj) or None for unsupported types."""
    if message.photo:
        return "photo", message.photo[-1]
    if message.video:
        return "video", message.video
    if message.video_note:
        return "video_note", message.video_note
    if message.animation:
        return "animation", message.animation
    if message.audio:
        return "audio", message.audio
    if message.voice:
        return "voice", message.voice
    if message.document:
        return "document", message.document
    if message.sticker:
        logger.info(
            "Sticker received, skipping per policy — chat %s message %s",
            message.chat_id, message.message_id,
        )
        return None
    return None


async def _warn_admin_once(context, chat_id: int, text: str) -> None:
    now = time.monotonic()
    last = _perm_warning_state.get(chat_id, 0.0)
    if now - last < _perm_warning_interval:
        return
    _perm_warning_state[chat_id] = now
    try:
        await context.bot.send_message(chat_id=chat_id, text=text)
    except TelegramError as exc:
        logger.error("Could not deliver admin warning to chat %s: %s", chat_id, exc)


async def _notify_duplicate_removed(
    context,
    chat_id: int,
    media_type: str,
    original_timestamp,
    message_to_delete_id: int | None = None,
) -> None:
    """Send and auto-delete a duplicate removal notification."""
    try:
        sent = await context.bot.send_message(
            chat_id=chat_id,
            text=(
                f"Duplicate {media_type} removed. "
                f"Original sent at {original_timestamp}."
            ),
        )
        if message_to_delete_id is not None:
            await asyncio.sleep(10)
            try:
                await context.bot.delete_message(
                    chat_id=chat_id, message_id=sent.message_id
                )
            except (Forbidden, BadRequest):
                pass
    except TelegramError as exc:
        logger.error("Failed to send duplicate notification: %s", exc)


async def handle_media(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if message is None or message.chat is None:
        return
    chat_id = message.chat_id

    classification = _classify(message)
    if classification is None:
        return
    file_type, tg_file = classification

    user_id = message.from_user.id if message.from_user else None
    if _user_rate_check(user_id or 0) and user_id not in _user_spam_notified:
        _user_spam_notified.add(user_id)
        logger.warning("User %s flagged as spam suspect (media rate exceeded)", user_id)
        admins = await context.bot.get_chat_administrators(chat_id)
        admin_ids = [a.user.id for a in admins if not a.user.is_bot]
        for admin_id in admin_ids[:1]:
            try:
                await context.bot.send_message(
                    chat_id=admin_id,
                    text=f"⚠️ User {user_id} is sending media very frequently in chat {chat_id}.",
                )
            except TelegramError:
                pass
        return

    factory = get_sessionmaker()
    try:
        async with factory() as session:
            settings = await crud.get_chat_settings(session, chat_id)
            enabled = settings.enabled
            threshold = settings.threshold
            video_threshold = settings.video_threshold
    except Exception as exc:
        logger.exception("Failed to load chat settings for %s: %s", chat_id, exc)
        return

    if not enabled:
        return

    if _rate_limited(chat_id):
        logger.warning(
            "Rate limit hit for chat %s — skipping message %s", chat_id, message.message_id
        )
        return

    tmp_path = os.path.join(download_dir, f"{uuid.uuid4().hex}_{file_type}")
    try:
        if hasattr(tg_file, "file_size") and tg_file.file_size and tg_file.file_size > TG_MAX_FILE_SIZE:
            logger.info("Skipping oversized file (%d bytes > 20MB) chat=%s msg=%s", tg_file.file_size, chat_id, message.message_id)
            return
        try:
            tg_file_obj = await context.bot.get_file(tg_file.file_id)
            await tg_file_obj.download_to_drive(custom_path=tmp_path)
        except TelegramError as exc:
            if "too big" in str(exc).lower():
                logger.info("Skipping oversized file chat %s msg %s: %s", chat_id, message.message_id, exc)
            else:
                logger.error("Download failed for chat %s msg %s: %s", chat_id, message.message_id, exc)
            return

        if not os.path.isfile(tmp_path) or os.path.getsize(tmp_path) == 0:
            logger.warning("Downloaded file is empty or missing for chat %s msg %s", chat_id, message.message_id)
            return

        doc_extra = {}
        if file_type == "document":
            doc_extra = {
                "file_name": tg_file.file_name if hasattr(tg_file, "file_name") else None,
                "mime_type": tg_file.mime_type if hasattr(tg_file, "mime_type") else None,
                "file_size": tg_file.file_size if hasattr(tg_file, "file_size") else None,
            }
        elif file_type in ("audio", "voice"):
            doc_extra = {
                "file_name": tg_file.file_name if hasattr(tg_file, "file_name") else None,
                "mime_type": tg_file.mime_type if hasattr(tg_file, "mime_type") else None,
                "file_size": tg_file.file_size if hasattr(tg_file, "file_size") else None,
            }

        try:
            async with factory() as session:
                result = await process_media(
                    file_path=tmp_path,
                    file_type=file_type,
                    chat_id=chat_id,
                    file_id=tg_file.file_id,
                    message_id=message.message_id,
                    sender_id=message.from_user.id if message.from_user else None,
                    threshold=threshold,
                    session=session,
                    cache=hash_cache,
                    video_threshold=video_threshold,
                    cleanup=False,
                    **doc_extra,
                )
                if result["is_duplicate"]:
                    confidence = result.get("confidence", "HARD")
                    if confidence == "SOFT" and file_type in ("audio", "voice", "document"):
                        await crud.log_duplicate(
                            session,
                            chat_id=chat_id,
                            original_message_id=0,
                            duplicate_message_id=message.message_id,
                            detection_method=result["method"] or "unknown",
                            confidence="SOFT",
                            action_taken="WARNED",
                            hamming_distance=result.get("hamming_distance"),
                            media_type=file_type,
                            sender_id=message.from_user.id if message.from_user else None,
                        )
                        logger.info(
                            "Soft duplicate flagged — chat=%s msg=%s method=%s",
                            chat_id, message.message_id, result["method"],
                        )
                    else:
                        await _delete_duplicate(context, chat_id, message.message_id, result)
                        await crud.update_chat_settings(
                            session, chat_id, duplicates_deleted_delta=1
                        )
                        await crud.log_duplicate(
                            session,
                            chat_id=chat_id,
                            original_message_id=0,
                            duplicate_message_id=message.message_id,
                            detection_method=result["method"] or "unknown",
                            confidence=confidence,
                            action_taken="DELETED",
                            hamming_distance=result.get("hamming_distance"),
                            media_type=file_type,
                            sender_id=message.from_user.id if message.from_user else None,
                        )
                        if notify_on_delete:
                            now = time.monotonic()
                            if now - _notify_cooldown.get(chat_id, 0) > _NOTIFY_COOLDOWN:
                                _notify_cooldown[chat_id] = now
                                asyncio.create_task(
                                    _notify_duplicate_removed(
                                        context,
                                        chat_id,
                                        file_type,
                                        message.date.isoformat() if message.date else "unknown",
                                    )
                                )
                await session.commit()
        except Exception as exc:
            logger.exception(
                "Processing failed for chat %s msg %s: %s", chat_id, message.message_id, exc
            )
    finally:
        try:
            if os.path.isfile(tmp_path):
                os.remove(tmp_path)
        except OSError:
            logger.warning("Could not remove temp file %s", tmp_path)


async def _delete_duplicate(context, chat_id: int, message_id: int, result: dict) -> None:
    method = result["method"]
    distance = result["hamming_distance"]
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
        logger.info(
            "Deleted duplicate chat=%s msg=%s method=%s hamming=%s matched=%s",
            chat_id, message_id, method, distance, result.get("matched_file_id"),
        )
    except (Forbidden, BadRequest) as exc:
        logger.error(
            "Cannot delete message in chat %s (msg %s): %s", chat_id, message_id, exc
        )
        await _warn_admin_once(
            context,
            chat_id,
            "⚠️ I detected a duplicate but I lack the *Delete messages* "
            "permission. Please grant it so I can clean duplicates.",
        )
    except TelegramError as exc:
        logger.error("Telegram error while deleting %s/%s: %s", chat_id, message_id, exc)


async def is_caller_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    msg = update.effective_message
    if msg is None or msg.from_user is None or msg.chat is None:
        return False
    try:
        member = await context.bot.get_chat_member(msg.chat_id, msg.from_user.id)
    except TelegramError as exc:
        logger.error("get_chat_member failed: %s", exc)
        return False
    return member.status in (ChatMemberStatus.OWNER, ChatMemberStatus.ADMINISTRATOR)
