"""Core domain vocabulary shared by the API, repository and pipeline layers."""

import hashlib
import unicodedata
from enum import StrEnum


class DocumentStatus(StrEnum):
    QUEUED = "queued"
    PROCESSING = "processing"
    ENRICHING = "enriching"
    COMPLETED = "completed"
    FAILED = "failed"


ACTIVE_STATUSES: tuple[DocumentStatus, ...] = (
    DocumentStatus.QUEUED,
    DocumentStatus.PROCESSING,
    DocumentStatus.ENRICHING,
)
TERMINAL_STATUSES: tuple[DocumentStatus, ...] = (DocumentStatus.COMPLETED, DocumentStatus.FAILED)


class Stage(StrEnum):
    """Pipeline stages, in execution order. The value doubles as the document status while running."""

    PROCESSING = "processing"
    ENRICHING = "enriching"


STAGE_ORDER: tuple[Stage, ...] = (Stage.PROCESSING, Stage.ENRICHING)


class StageState(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


def normalize_content(content: str) -> str:
    """Canonical form used for hashing: NFC unicode + trimmed outer whitespace.

    Internal whitespace is preserved on purpose: it can be meaningful (code, poetry), and
    over-normalising risks two genuinely different documents sharing a cached result.
    """
    return unicodedata.normalize("NFC", content).strip()


def content_hash(content: str) -> str:
    return hashlib.sha256(normalize_content(content).encode("utf-8")).hexdigest()
