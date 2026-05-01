"""CRUD operations for media hashes and chat settings."""
from __future__ import annotations

from datetime import datetime
from typing import Sequence

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from bot.db.models import ChatSettings, DuplicateLog, MediaHash


def _utcnow() -> datetime:
    return datetime.utcnow()


async def create_hash(
    session: AsyncSession,
    *,
    file_hash: str,
    phash: str | None = None,
    file_type: str,
    file_id: str,
    chat_id: int,
    message_id: int,
    sender_id: int | None,
    secondary_hash: str | None = None,
    file_name: str | None = None,
    mime_type: str | None = None,
    file_size: int | None = None,
    duration_seconds: float | None = None,
    detection_method: str = "sha256",
) -> MediaHash:
    """Insert a new media hash row and return it."""
    row = MediaHash(
        file_hash=file_hash,
        phash=phash,
        file_type=file_type,
        file_id=file_id,
        chat_id=chat_id,
        message_id=message_id,
        sender_id=sender_id,
        secondary_hash=secondary_hash,
        file_name=file_name,
        mime_type=mime_type,
        file_size=file_size,
        duration_seconds=duration_seconds,
        detection_method=detection_method,
        timestamp=_utcnow(),
    )
    session.add(row)
    await session.flush()
    return row


async def get_hash_by_chat(
    session: AsyncSession, chat_id: int, file_hash: str
) -> MediaHash | None:
    """Return the first matching (chat_id, file_hash) row, or None."""
    stmt = (
        select(MediaHash)
        .where(MediaHash.chat_id == chat_id, MediaHash.file_hash == file_hash)
        .limit(1)
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def get_by_secondary_hash(
    session: AsyncSession, chat_id: int, secondary_hash: str
) -> MediaHash | None:
    """Return the first matching (chat_id, secondary_hash) row, or None."""
    stmt = (
        select(MediaHash)
        .where(
            MediaHash.chat_id == chat_id,
            MediaHash.secondary_hash == secondary_hash,
            MediaHash.secondary_hash.is_not(None),
        )
        .limit(1)
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def get_all_phashes_for_chat(
    session: AsyncSession, chat_id: int
) -> list[tuple[str, str]]:
    """Return list of (phash, file_id) tuples for all images stored in chat."""
    stmt = (
        select(MediaHash.phash, MediaHash.file_id)
        .where(MediaHash.chat_id == chat_id, MediaHash.phash.is_not(None))
    )
    result = await session.execute(stmt)
    return [(row[0], row[1]) for row in result.all() if row[0] is not None]


async def get_all_secondary_hashes_for_chat(
    session: AsyncSession, chat_id: int
) -> list[tuple[str, str]]:
    """Return list of (secondary_hash, file_id) for a chat."""
    stmt = (
        select(MediaHash.secondary_hash, MediaHash.file_id)
        .where(
            MediaHash.chat_id == chat_id,
            MediaHash.secondary_hash.is_not(None),
        )
    )
    result = await session.execute(stmt)
    return [(row[0], row[1]) for row in result.all() if row[0] is not None]


async def get_all_hashes_for_chat(
    session: AsyncSession, chat_id: int
) -> list[tuple[str, str | None]]:
    """Return list of (file_hash, phash) tuples for a chat (used to warm cache)."""
    stmt = select(MediaHash.file_hash, MediaHash.phash).where(
        MediaHash.chat_id == chat_id
    )
    result = await session.execute(stmt)
    return [(row[0], row[1]) for row in result.all()]


async def get_suspicious_by_name_size(
    session: AsyncSession,
    chat_id: int,
    file_name: str,
    file_size: int,
    size_tolerance_pct: float = 5.0,
) -> list[MediaHash]:
    """Find entries with same file_name and size within tolerance but different SHA-256."""
    min_size = int(file_size * (1 - size_tolerance_pct / 100))
    max_size = int(file_size * (1 + size_tolerance_pct / 100))
    stmt = (
        select(MediaHash)
        .where(
            MediaHash.chat_id == chat_id,
            MediaHash.file_name == file_name,
            MediaHash.file_size.between(min_size, max_size),
        )
    )
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def get_audio_near_matches(
    session: AsyncSession,
    chat_id: int,
    duration: float,
    file_size: int,
    duration_tolerance: float = 2.0,
    size_tolerance_pct: float = 5.0,
) -> list[MediaHash]:
    """Find audio entries with similar duration and size."""
    min_dur = duration - duration_tolerance
    max_dur = duration + duration_tolerance
    min_size = int(file_size * (1 - size_tolerance_pct / 100))
    max_size = int(file_size * (1 + size_tolerance_pct / 100))
    stmt = (
        select(MediaHash)
        .where(
            MediaHash.chat_id == chat_id,
            MediaHash.file_type.in_(["audio", "voice"]),
            MediaHash.duration_seconds.between(min_dur, max_dur),
            MediaHash.file_size.between(min_size, max_size),
        )
    )
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def delete_hashes_for_chat(session: AsyncSession, chat_id: int) -> int:
    """Delete all hash rows for a chat. Returns deleted row count."""
    stmt = delete(MediaHash).where(MediaHash.chat_id == chat_id)
    result = await session.execute(stmt)
    return result.rowcount or 0


async def count_hashes_for_chat(session: AsyncSession, chat_id: int) -> int:
    """Total hash rows stored for a chat."""
    stmt = select(func.count()).select_from(MediaHash).where(
        MediaHash.chat_id == chat_id
    )
    result = await session.execute(stmt)
    return int(result.scalar_one())


async def get_chat_settings(
    session: AsyncSession, chat_id: int
) -> ChatSettings:
    """Return the chat settings row, creating a default one if absent."""
    stmt = select(ChatSettings).where(ChatSettings.chat_id == chat_id)
    result = await session.execute(stmt)
    row = result.scalar_one_or_none()
    if row is None:
        row = ChatSettings(chat_id=chat_id, enabled=True, threshold=10, video_threshold=8)
        session.add(row)
        await session.flush()
    return row


async def update_chat_settings(
    session: AsyncSession,
    chat_id: int,
    *,
    enabled: bool | None = None,
    threshold: int | None = None,
    video_threshold: int | None = None,
    duplicates_deleted_delta: int | None = None,
) -> ChatSettings:
    """Update fields on chat settings, creating row if missing."""
    row = await get_chat_settings(session, chat_id)
    if enabled is not None:
        row.enabled = enabled
    if threshold is not None:
        row.threshold = threshold
    if video_threshold is not None:
        row.video_threshold = video_threshold
    if duplicates_deleted_delta:
        row.duplicates_deleted = (row.duplicates_deleted or 0) + duplicates_deleted_delta
    row.updated_at = _utcnow()
    await session.flush()
    return row


async def log_duplicate(
    session: AsyncSession,
    *,
    chat_id: int,
    original_message_id: int,
    duplicate_message_id: int,
    detection_method: str,
    confidence: str,
    action_taken: str,
    hamming_distance: int | None = None,
    media_type: str | None = None,
    sender_id: int | None = None,
) -> DuplicateLog:
    """Insert a duplicate detection audit log entry."""
    row = DuplicateLog(
        chat_id=chat_id,
        original_message_id=original_message_id,
        duplicate_message_id=duplicate_message_id,
        detection_method=detection_method,
        confidence=confidence,
        action_taken=action_taken,
        hamming_distance=hamming_distance,
        media_type=media_type,
        sender_id=sender_id,
        timestamp=_utcnow(),
    )
    session.add(row)
    await session.flush()
    return row


async def top_duplicate_senders(
    session: AsyncSession, chat_id: int, limit: int = 5
) -> list[tuple[int | None, int]]:
    """Return top N sender_ids by duplicate count in a chat."""
    stmt = (
        select(DuplicateLog.sender_id, func.count(DuplicateLog.id))
        .where(DuplicateLog.chat_id == chat_id)
        .group_by(DuplicateLog.sender_id)
        .order_by(func.count(DuplicateLog.id).desc())
        .limit(limit)
    )
    result = await session.execute(stmt)
    return [(row[0], row[1]) for row in result.all()]


async def duplicates_by_media_type(
    session: AsyncSession, chat_id: int
) -> list[tuple[str | None, int]]:
    """Return duplicate counts grouped by media_type for a chat."""
    stmt = (
        select(DuplicateLog.media_type, func.count(DuplicateLog.id))
        .where(DuplicateLog.chat_id == chat_id)
        .group_by(DuplicateLog.media_type)
    )
    result = await session.execute(stmt)
    return [(row[0], row[1]) for row in result.all()]


async def global_stats(session: AsyncSession) -> dict:
    """Return aggregate stats across all chats."""
    total_hashes = await session.execute(select(func.count()).select_from(MediaHash))
    total_dupes = await session.execute(
        select(func.coalesce(func.sum(ChatSettings.duplicates_deleted), 0))
    )
    return {
        "total_unique_media": int(total_hashes.scalar_one()),
        "total_duplicates_deleted": int(total_dupes.scalar_one()),
    }


async def per_chat_breakdown(
    session: AsyncSession,
) -> Sequence[tuple[int, int, int]]:
    """Return list of (chat_id, unique_stored, duplicates_deleted)."""
    stmt = select(
        MediaHash.chat_id,
        func.count(MediaHash.id),
    ).group_by(MediaHash.chat_id)
    counts = {row[0]: row[1] for row in (await session.execute(stmt)).all()}

    settings_stmt = select(ChatSettings.chat_id, ChatSettings.duplicates_deleted)
    dupes = {row[0]: row[1] for row in (await session.execute(settings_stmt)).all()}

    chat_ids = set(counts) | set(dupes)
    return [(cid, counts.get(cid, 0), dupes.get(cid, 0)) for cid in chat_ids]
