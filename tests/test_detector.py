"""Tests for the duplicate detector."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from bot.core.cache import HashCache
from bot.core.detector import (
    is_exact_duplicate,
    is_perceptual_duplicate,
    process_media,
)
from bot.db import crud

pytestmark = pytest.mark.asyncio


async def _write_image(tmp_path: Path, name: str, color=(10, 20, 30)) -> str:
    from PIL import Image

    p = tmp_path / name
    Image.new("RGB", (64, 64), color=color).save(p)
    return str(p)


async def test_no_duplicate_when_db_empty(session, tmp_path):
    cache = HashCache()
    f = await _write_image(tmp_path, "a.png")
    result = await process_media(
        f, "photo", chat_id=1, file_id="F1", message_id=1,
        sender_id=None, threshold=10, session=session, cache=cache,
    )
    await session.commit()
    assert result["is_duplicate"] is False
    assert not Path(f).exists()


async def test_exact_duplicate_detected(session, tmp_path):
    cache = HashCache()
    f1 = await _write_image(tmp_path, "a.png")
    f2 = await _write_image(tmp_path, "b.png")
    # Make both files byte-identical
    Path(f2).write_bytes(Path(f1).read_bytes())

    r1 = await process_media(
        f1, "photo", 1, "F1", 1, None, 10, session, cache,
    )
    await session.commit()
    r2 = await process_media(
        f2, "photo", 1, "F2", 2, None, 10, session, cache,
    )
    await session.commit()
    assert r1["is_duplicate"] is False
    assert r2["is_duplicate"] is True
    assert r2["method"] == "exact"


async def test_perceptual_duplicate_within_threshold(session, tmp_path):
    cache = HashCache()
    f1 = await _write_image(tmp_path, "a.png", color=(10, 20, 30))
    f2 = await _write_image(tmp_path, "b.png", color=(11, 21, 31))  # near-identical

    await process_media(f1, "photo", 1, "F1", 1, None, 10, session, cache)
    await session.commit()
    r = await process_media(f2, "photo", 1, "F2", 2, None, 10, session, cache)
    await session.commit()
    assert r["is_duplicate"] is True
    assert r["method"] == "perceptual"
    assert r["hamming_distance"] is not None
    assert r["hamming_distance"] <= 10


async def test_perceptual_no_match_with_low_threshold(session, tmp_path):
    from PIL import Image
    import random

    cache = HashCache()
    rnd = random.Random(1)
    img1 = Image.new("RGB", (64, 64))
    img1.putdata([(rnd.randint(0, 255),) * 3 for _ in range(64 * 64)])
    f1 = tmp_path / "a.png"
    img1.save(f1)

    rnd2 = random.Random(99)
    img2 = Image.new("RGB", (64, 64))
    img2.putdata([(rnd2.randint(0, 255),) * 3 for _ in range(64 * 64)])
    f2 = tmp_path / "b.png"
    img2.save(f2)

    await process_media(str(f1), "photo", 1, "F1", 1, None, 10, session, cache)
    await session.commit()
    r = await process_media(str(f2), "photo", 1, "F2", 2, None, 0, session, cache)
    await session.commit()
    assert r["is_duplicate"] is False


async def test_is_perceptual_duplicate_empty_list(session):
    cache = HashCache()
    dup, dist, matched = await is_perceptual_duplicate(
        99, "0" * 16, 10, session, cache
    )
    assert dup is False
    assert dist is None
    assert matched is None


async def test_cache_avoids_second_db_query(session, tmp_path):
    cache = HashCache()
    f1 = await _write_image(tmp_path, "a.png")
    await process_media(f1, "photo", 1, "F1", 1, None, 10, session, cache)
    await session.commit()

    # Spy on the DB function — second exact-duplicate check should hit cache only.
    with patch(
        "bot.core.detector.crud.get_all_hashes_for_chat",
        new=AsyncMock(side_effect=AssertionError("DB queried again")),
    ):
        # Cache is already warm, so this must NOT call the patched function.
        assert await is_exact_duplicate(1, "deadbeef" * 8, session, cache) is False


async def test_temp_file_cleaned_on_error(session, tmp_path, monkeypatch):
    cache = HashCache()
    f = await _write_image(tmp_path, "a.png")

    def boom(_path):
        raise RuntimeError("hash failure")

    monkeypatch.setattr("bot.core.detector.compute_sha256", boom)
    with pytest.raises(RuntimeError):
        await process_media(f, "photo", 1, "F1", 1, None, 10, session, cache)
    assert not Path(f).exists()


async def test_unsupported_file_type_no_phash(session, tmp_path):
    cache = HashCache()
    f = tmp_path / "doc.bin"
    f.write_bytes(b"hello world")
    r = await process_media(
        str(f), "document", 1, "F1", 1, None, 10, session, cache,
    )
    await session.commit()
    assert r["phash"] is None
    assert r["is_duplicate"] is False


async def test_is_exact_duplicate_direct(session):
    cache = HashCache()
    await crud.create_hash(
        session, file_hash="abc", phash=None, file_type="photo",
        file_id="F", chat_id=7, message_id=1, sender_id=None,
    )
    await session.commit()
    assert await is_exact_duplicate(7, "abc", session, cache) is True
    assert await is_exact_duplicate(7, "xyz", session, cache) is False
