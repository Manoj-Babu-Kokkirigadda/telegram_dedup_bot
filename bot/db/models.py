"""SQLAlchemy ORM models for the dedup bot."""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Float,
    Index,
    Integer,
    String,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Declarative base for all ORM models."""


def _utcnow() -> datetime:
    return datetime.utcnow()


class MediaHash(Base):
    """Stored hash record for a single piece of media seen in a chat."""

    __tablename__ = "media_hashes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    file_hash: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    phash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    secondary_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    file_type: Mapped[str] = mapped_column(String(20), nullable=False)
    file_id: Mapped[str] = mapped_column(String(200), nullable=False)
    chat_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    message_id: Mapped[int] = mapped_column(Integer, nullable=False)
    sender_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    file_name: Mapped[str | None] = mapped_column(String(500), nullable=True)
    mime_type: Mapped[str | None] = mapped_column(String(100), nullable=True)
    file_size: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    duration_seconds: Mapped[float | None] = mapped_column(Float, nullable=True)
    detection_method: Mapped[str] = mapped_column(
        String(20), nullable=False, default="sha256"
    )
    timestamp: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=_utcnow
    )

    __table_args__ = (
        Index("ix_media_hashes_chat_file", "chat_id", "file_hash"),
        Index("ix_media_hashes_chat_secondary", "chat_id", "secondary_hash"),
    )


class ChatSettings(Base):
    """Per-chat configuration for the dedup bot."""

    __tablename__ = "chat_settings"

    chat_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    threshold: Mapped[int] = mapped_column(Integer, nullable=False, default=10)
    video_threshold: Mapped[int] = mapped_column(Integer, nullable=False, default=8)
    duplicates_deleted: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=_utcnow, onupdate=_utcnow
    )


class DuplicateLog(Base):
    """Audit log of every duplicate detection event."""

    __tablename__ = "duplicate_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    chat_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    original_message_id: Mapped[int] = mapped_column(Integer, nullable=False)
    duplicate_message_id: Mapped[int] = mapped_column(Integer, nullable=False)
    detection_method: Mapped[str] = mapped_column(String(20), nullable=False)
    confidence: Mapped[str] = mapped_column(String(10), nullable=False)
    action_taken: Mapped[str] = mapped_column(String(10), nullable=False)
    hamming_distance: Mapped[int | None] = mapped_column(Integer, nullable=True)
    media_type: Mapped[str | None] = mapped_column(String(20), nullable=True)
    sender_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    timestamp: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=_utcnow
    )

    __table_args__ = (
        Index("ix_duplicate_log_chat", "chat_id", "timestamp"),
    )
