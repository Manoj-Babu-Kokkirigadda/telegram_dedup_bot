"""Duplicate detection orchestration."""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from bot.core.cache import HashCache
from bot.core.hasher import (
    compute_phash,
    compute_sha256,
    compute_video_phash,
    get_audio_duration,
    hamming_distance_hex,
)
from bot.db import crud

logger = logging.getLogger(__name__)


@dataclass
class DetectionResult:
    is_duplicate: bool
    method: str | None
    hamming_distance: int | None
    file_hash: str
    phash: str | None
    secondary_hash: str | None = None
    matched_file_id: str | None = None
    confidence: str | None = None
    duration_seconds: float | None = None
    file_name: str | None = None
    mime_type: str | None = None
    file_size: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "is_duplicate": self.is_duplicate,
            "method": self.method,
            "hamming_distance": self.hamming_distance,
            "file_hash": self.file_hash,
            "phash": self.phash,
            "secondary_hash": self.secondary_hash,
            "matched_file_id": self.matched_file_id,
            "confidence": self.confidence,
            "duration_seconds": self.duration_seconds,
            "file_name": self.file_name,
            "mime_type": self.mime_type,
            "file_size": self.file_size,
        }


async def _ensure_cache(
    chat_id: int, session: AsyncSession, cache: HashCache
) -> set[tuple[str, str | None]]:
    cached = cache.get_hashes(chat_id)
    if cached is not None:
        logger.info("cache hit: chat=%s (%d entries)", chat_id, len(cached))
        return cached
    rows = await crud.get_all_hashes_for_chat(session, chat_id)
    cache.set_hashes(chat_id, rows)
    logger.info("cache miss (warmed from DB): chat=%s (%d entries)", chat_id, len(rows))
    return cache.get_hashes(chat_id) or set()


async def _ensure_secondary_cache(
    chat_id: int, session: AsyncSession, cache: HashCache
) -> set[tuple[str, str]]:
    """Ensure secondary hashes (video thumbnails) are cached for a chat."""
    cached = cache.get_secondary_hashes(chat_id)
    if cached is not None:
        logger.info("secondary cache hit: chat=%s (%d entries)", chat_id, len(cached))
        return cached
    rows = await crud.get_all_secondary_hashes_for_chat(session, chat_id)
    cache.set_secondary_hashes(chat_id, rows)
    logger.info("secondary cache miss (warmed from DB): chat=%s (%d entries)", chat_id, len(rows))
    return cache.get_secondary_hashes(chat_id) or set()


async def is_exact_duplicate(
    chat_id: int, file_hash: str, session: AsyncSession, cache: HashCache
) -> bool:
    cached = await _ensure_cache(chat_id, session, cache)
    if any(fh == file_hash for fh, _ in cached):
        return True
    return (await crud.get_hash_by_chat(session, chat_id, file_hash)) is not None


async def is_perceptual_duplicate(
    chat_id: int,
    phash: str | None,
    threshold: int,
    session: AsyncSession,
    cache: HashCache,
) -> tuple[bool, int | None, str | None]:
    """Return (is_duplicate, min_distance, matched_phash_string)."""
    if not phash:
        return False, None, None
    cached = await _ensure_cache(chat_id, session, cache)
    candidates = [ph for _, ph in cached if ph]
    if not candidates:
        return False, None, None
    best: tuple[int, str] | None = None
    for stored in candidates:
        d = hamming_distance_hex(phash, stored)
        if best is None or d < best[0]:
            best = (d, stored)
    assert best is not None
    if best[0] <= threshold:
        return True, best[0], best[1]
    return False, best[0], None


async def process_media(
    file_path: str,
    file_type: str,
    chat_id: int,
    file_id: str,
    message_id: int,
    sender_id: int | None,
    threshold: int,
    session: AsyncSession,
    cache: HashCache,
    *,
    video_threshold: int = 8,
    cleanup: bool = True,
    file_name: str | None = None,
    mime_type: str | None = None,
    file_size: int | None = None,
) -> dict[str, Any]:
    """Hash + detect + persist for all supported media types.

    Always cleans up the temp file unless cleanup=False.
    """
    try:
        file_hash = compute_sha256(file_path)

        # --- EXACT DUPLICATE CHECK (all types) ---
        if await is_exact_duplicate(chat_id, file_hash, session, cache):
            return DetectionResult(
                is_duplicate=True,
                method="exact",
                hamming_distance=0,
                file_hash=file_hash,
                phash=None,
                confidence="HARD",
                file_name=file_name,
                mime_type=mime_type,
                file_size=file_size,
            ).to_dict()

        # --- TYPE-SPECIFIC SECONDARY DETECTION ---

        if file_type == "photo":
            phash = compute_phash(file_path)
            if phash:
                dup, dist, matched = await is_perceptual_duplicate(
                    chat_id, phash, threshold, session, cache
                )
                if dup:
                    return DetectionResult(
                        is_duplicate=True,
                        method="perceptual",
                        hamming_distance=dist,
                        file_hash=file_hash,
                        phash=phash,
                        matched_file_id=matched,
                        confidence="HARD",
                    ).to_dict()

            await crud.create_hash(
                session,
                file_hash=file_hash,
                phash=phash,
                file_type=file_type,
                file_id=file_id,
                chat_id=chat_id,
                message_id=message_id,
                sender_id=sender_id,
                detection_method="sha256",
            )
            cache.add(chat_id, file_hash, phash)
            return DetectionResult(
                is_duplicate=False,
                method=None,
                hamming_distance=None,
                file_hash=file_hash,
                phash=phash,
            ).to_dict()

        elif file_type in ("video", "animation", "video_note"):
            secondary_hash = compute_video_phash(file_path)
            if secondary_hash:
                sec_cached = await _ensure_secondary_cache(chat_id, session, cache)
                if any(sh == secondary_hash for sh, _ in sec_cached):
                    return DetectionResult(
                        is_duplicate=True,
                        method="video_thumb",
                        hamming_distance=0,
                        file_hash=file_hash,
                        phash=None,
                        secondary_hash=secondary_hash,
                        confidence="HARD",
                    ).to_dict()

                candidates = [sh for sh, _ in sec_cached if sh]
                if candidates:
                    best: tuple[int, str] | None = None
                    for stored in candidates:
                        d = hamming_distance_hex(secondary_hash, stored)
                        if best is None or d < best[0]:
                            best = (d, stored)
                    if best is not None and best[0] <= video_threshold:
                        return DetectionResult(
                            is_duplicate=True,
                            method="video_thumb",
                            hamming_distance=best[0],
                            file_hash=file_hash,
                            phash=None,
                            secondary_hash=secondary_hash,
                            matched_file_id=best[1],
                            confidence="HARD",
                        ).to_dict()

            await crud.create_hash(
                session,
                file_hash=file_hash,
                phash=None,
                file_type=file_type,
                file_id=file_id,
                chat_id=chat_id,
                message_id=message_id,
                sender_id=sender_id,
                secondary_hash=secondary_hash,
                detection_method="sha256",
            )
            cache.add(chat_id, file_hash, None)
            if secondary_hash:
                cache.add_secondary(chat_id, secondary_hash, file_id)
            return DetectionResult(
                is_duplicate=False,
                method=None,
                hamming_distance=None,
                file_hash=file_hash,
                phash=None,
                secondary_hash=secondary_hash,
            ).to_dict()

        elif file_type in ("audio", "voice"):
            duration = get_audio_duration(file_path)
            is_probable_dup = False
            matched_original: Any | None = None

            if duration is not None and file_size is not None:
                near_matches = await crud.get_audio_near_matches(
                    session, chat_id, duration, file_size
                )
                if near_matches:
                    is_probable_dup = True
                    matched_original = near_matches[0]

            if is_probable_dup and matched_original is not None:
                return DetectionResult(
                    is_duplicate=True,
                    method="audio_soft",
                    hamming_distance=None,
                    file_hash=file_hash,
                    phash=None,
                    matched_file_id=matched_original.file_id,
                    confidence="SOFT",
                    duration_seconds=duration,
                    file_name=file_name,
                    mime_type=mime_type,
                    file_size=file_size,
                ).to_dict()

            await crud.create_hash(
                session,
                file_hash=file_hash,
                phash=None,
                file_type=file_type,
                file_id=file_id,
                chat_id=chat_id,
                message_id=message_id,
                sender_id=sender_id,
                duration_seconds=duration,
                file_name=file_name,
                mime_type=mime_type,
                file_size=file_size,
                detection_method="sha256",
            )
            cache.add(chat_id, file_hash, None)
            return DetectionResult(
                is_duplicate=False,
                method=None,
                hamming_distance=None,
                file_hash=file_hash,
                phash=None,
                duration_seconds=duration,
                file_name=file_name,
                mime_type=mime_type,
                file_size=file_size,
            ).to_dict()

        elif file_type == "document":
            suspicious = await crud.get_suspicious_by_name_size(
                session, chat_id, file_name or "", file_size or 0
            )
            if suspicious:
                return DetectionResult(
                    is_duplicate=True,
                    method="exact",
                    hamming_distance=None,
                    file_hash=file_hash,
                    phash=None,
                    matched_file_id=suspicious[0].file_id,
                    confidence="SOFT",
                    file_name=file_name,
                    mime_type=mime_type,
                    file_size=file_size,
                ).to_dict()

            await crud.create_hash(
                session,
                file_hash=file_hash,
                phash=None,
                file_type=file_type,
                file_id=file_id,
                chat_id=chat_id,
                message_id=message_id,
                sender_id=sender_id,
                file_name=file_name,
                mime_type=mime_type,
                file_size=file_size,
                detection_method="sha256",
            )
            cache.add(chat_id, file_hash, None)
            return DetectionResult(
                is_duplicate=False,
                method=None,
                hamming_distance=None,
                file_hash=file_hash,
                phash=None,
                file_name=file_name,
                mime_type=mime_type,
                file_size=file_size,
            ).to_dict()

        else:
            await crud.create_hash(
                session,
                file_hash=file_hash,
                phash=None,
                file_type=file_type,
                file_id=file_id,
                chat_id=chat_id,
                message_id=message_id,
                sender_id=sender_id,
                detection_method="sha256",
            )
            cache.add(chat_id, file_hash, None)
            return DetectionResult(
                is_duplicate=False,
                method=None,
                hamming_distance=None,
                file_hash=file_hash,
                phash=None,
            ).to_dict()

    finally:
        if cleanup:
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
            except OSError as exc:
                logger.warning("Could not remove temp file %s: %s", file_path, exc)
