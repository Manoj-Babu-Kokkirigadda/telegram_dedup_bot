"""Tests for bot.db.crud."""
from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import inspect

from bot.db import crud
from bot.db.models import Base, MediaHash


pytestmark = pytest.mark.asyncio


async def test_create_and_get_hash(session):
    row = await crud.create_hash(
        session,
        file_hash="a" * 64,
        phash=None,
        file_type="photo",
        file_id="FID1",
        chat_id=42,
        message_id=1,
        sender_id=7,
    )
    await session.commit()
    assert row.id is not None

    found = await crud.get_hash_by_chat(session, 42, "a" * 64)
    assert found is not None
    assert found.file_id == "FID1"

    missing = await crud.get_hash_by_chat(session, 42, "b" * 64)
    assert missing is None


async def test_phash_listing(session):
    await crud.create_hash(
        session,
        file_hash="h1",
        phash="p1",
        file_type="photo",
        file_id="F1",
        chat_id=1,
        message_id=1,
        sender_id=None,
    )
    await crud.create_hash(
        session,
        file_hash="h2",
        phash=None,
        file_type="video",
        file_id="F2",
        chat_id=1,
        message_id=2,
        sender_id=None,
    )
    await session.commit()
    phashes = await crud.get_all_phashes_for_chat(session, 1)
    assert phashes == [("p1", "F1")]


async def test_delete_hashes_for_chat(session):
    for i in range(3):
        await crud.create_hash(
            session,
            file_hash=f"h{i}",
            phash=None,
            file_type="photo",
            file_id=f"F{i}",
            chat_id=99,
            message_id=i,
            sender_id=None,
        )
    await session.commit()
    assert await crud.count_hashes_for_chat(session, 99) == 3
    deleted = await crud.delete_hashes_for_chat(session, 99)
    await session.commit()
    assert deleted == 3
    assert await crud.count_hashes_for_chat(session, 99) == 0


async def test_chat_settings_crud(session):
    s = await crud.get_chat_settings(session, 5)
    await session.commit()
    assert s.enabled is True
    assert s.threshold == 10

    await crud.update_chat_settings(session, 5, enabled=False, threshold=7)
    await session.commit()

    s2 = await crud.get_chat_settings(session, 5)
    assert s2.enabled is False
    assert s2.threshold == 7

    await crud.update_chat_settings(session, 5, duplicates_deleted_delta=2)
    await crud.update_chat_settings(session, 5, duplicates_deleted_delta=3)
    await session.commit()
    s3 = await crud.get_chat_settings(session, 5)
    assert s3.duplicates_deleted == 5


async def test_concurrent_writes(session_factory):
    async def writer(i: int):
        async with session_factory() as s:
            await crud.create_hash(
                s,
                file_hash=f"hash{i}",
                phash=None,
                file_type="photo",
                file_id=f"F{i}",
                chat_id=1,
                message_id=i,
                sender_id=None,
            )
            await s.commit()

    await asyncio.gather(*(writer(i) for i in range(10)))

    async with session_factory() as s:
        assert await crud.count_hashes_for_chat(s, 1) == 10


async def test_composite_index_present(session):
    def _check(sync_conn):
        insp = inspect(sync_conn)
        idx_names = {ix["name"] for ix in insp.get_indexes("media_hashes")}
        assert "ix_media_hashes_chat_file" in idx_names

    conn = await session.connection()
    await conn.run_sync(_check)


async def test_global_stats_and_breakdown(session):
    await crud.create_hash(
        session, file_hash="x", phash=None, file_type="photo",
        file_id="F", chat_id=1, message_id=1, sender_id=None,
    )
    await crud.update_chat_settings(session, 1, duplicates_deleted_delta=4)
    await session.commit()
    stats = await crud.global_stats(session)
    assert stats["total_unique_media"] == 1
    assert stats["total_duplicates_deleted"] == 4
    rows = await crud.per_chat_breakdown(session)
    assert (1, 1, 4) in rows


# Silence unused-import warning while keeping Base importable for fixtures.
_ = Base, MediaHash
