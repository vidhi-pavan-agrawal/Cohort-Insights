"""Mock stage logic + failure/latency simulation.

The *outputs* are deterministic functions of their input (content -> summary -> tags). That is what
makes content-hash caching sound: the same content always yields the same result.
The *latency and failures* are random and injected separately via a `StageSimulator`, so tests can
swap in a controllable one without touching business logic.
"""

import asyncio
import random
import re
from collections import Counter
from typing import Protocol

from app.core.config import Settings
from app.domain import Stage

_SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+")
_WORD = re.compile(r"[a-zA-Z][a-zA-Z0-9\-']{2,}")
_STOPWORDS = frozenset(
    (
        "a about above after again against all also am an and any are as at be because been before being "
        "below between both but by can could did do does doing down during each few for from further had "
        "has have having he her here hers herself him himself his how into is it its itself just me more "
        "most my myself no nor not now of off on once only or other our ours ourselves out over own same "
        "she should so some such than that the their theirs them themselves then there these they this "
        "those through too under until very was we were what when where which while who whom why will "
        "with would you your yours yourself yourselves summary upon via per"
    ).split()
)

SUMMARY_MAX_CHARS = 280
MAX_TAGS = 5


class StageError(Exception):
    """A (simulated) transient failure of one stage attempt."""


def summarize(content: str) -> str:
    """Stage 1 mock: the first two sentences, capped at SUMMARY_MAX_CHARS on a word boundary."""
    text = " ".join(content.split())
    sentences = _SENTENCE_SPLIT.split(text)
    summary = " ".join(sentences[:2])
    if len(summary) > SUMMARY_MAX_CHARS:
        summary = summary[:SUMMARY_MAX_CHARS].rsplit(" ", 1)[0].rstrip(",;:") + "…"
    return summary


def extract_tags(summary: str, limit: int = MAX_TAGS) -> list[str]:
    """Stage 2 mock: most frequent non-stopword terms of the *summary* (ties -> first occurrence)."""
    words = [w.lower().strip("'-") for w in _WORD.findall(summary)]
    words = [w for w in words if w and w not in _STOPWORDS]
    counts = Counter(words)
    first_seen = {w: i for i, w in reversed(list(enumerate(words)))}
    ranked = sorted(counts, key=lambda w: (-counts[w], first_seen[w]))
    return ranked[:limit]


class StageSimulator(Protocol):
    async def __call__(self, stage: Stage, doc_id: str, attempt: int) -> None:
        """Wait for the stage's simulated duration; raise StageError to simulate a failure."""


class RandomStageSimulator:
    """Production simulator: uniform random duration per stage, independent failure chance."""

    def __init__(self, settings: Settings, rng: random.Random | None = None) -> None:
        self._rng = rng or random.Random()
        self._failure_rate = settings.stage_failure_rate
        self._durations = {
            Stage.PROCESSING: (settings.processing_min_seconds, settings.processing_max_seconds),
            Stage.ENRICHING: (settings.enriching_min_seconds, settings.enriching_max_seconds),
        }

    async def __call__(self, stage: Stage, doc_id: str, attempt: int) -> None:
        low, high = self._durations[stage]
        await asyncio.sleep(self._rng.uniform(low, high))
        if self._rng.random() < self._failure_rate:
            raise StageError(f"simulated {stage.value} failure (attempt {attempt})")
