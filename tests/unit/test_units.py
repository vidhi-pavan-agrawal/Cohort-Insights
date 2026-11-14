import random

import pytest
from pydantic import ValidationError

from app.domain import content_hash
from app.pipeline.runner import PipelineRunner
from app.pipeline.stages import SUMMARY_MAX_CHARS, extract_tags, summarize
from app.schemas.documents import DocumentCreate, DocumentOut, DocumentPatch, to_document_out
from tests.conftest import make_settings


class TestContentHash:
    def test_stable_and_hex(self):
        assert content_hash("abc") == content_hash("abc")
        assert len(content_hash("abc")) == 64

    def test_outer_whitespace_and_unicode_normalized(self):
        assert content_hash("  café \n") == content_hash("cafe\u0301")

    def test_internal_whitespace_is_significant(self):
        assert content_hash("a  b") != content_hash("a b")


class TestMockStages:
    def test_summary_is_first_two_sentences(self):
        assert summarize("One. Two!   Three? Four.") == "One. Two!"

    def test_summary_is_capped_on_word_boundary(self):
        s = summarize("word " * 500)
        assert len(s) <= SUMMARY_MAX_CHARS + 1 and s.endswith("…")

    def test_tags_are_deterministic_ranked_and_stopword_free(self):
        summary = "Redis caches results. Redis is fast and the cache helps MongoDB."
        tags = extract_tags(summary)
        assert tags[0] == "redis"
        assert "the" not in tags and "and" not in tags
        assert tags == extract_tags(summary)
        assert len(extract_tags("alpha beta gamma delta epsilon zeta eta theta")) == 5


class TestSchemas:
    def base(self, **kw):
        return {"user_id": "alice", "title": "t", "content": "c", **kw}

    @pytest.mark.parametrize(
        "override",
        [
            {"content": "   \n\t"},
            {"content": ""},
            {"title": "   "},
            {"user_id": "bad user!"},
            {"client_doc_ref": "has space"},
            {"ref_version": 1},  # without client_doc_ref
            {"ref_version": -1, "client_doc_ref": "r"},
            {"unexpected": "field"},
            {"content": "nul\x00byte"},
        ],
    )
    def test_rejects_invalid_create(self, override):
        with pytest.raises(ValidationError):
            DocumentCreate(**self.base(**override))

    def test_title_is_trimmed(self):
        assert DocumentCreate(**self.base(title="  hi  ")).title == "hi"

    def test_patch_rejects_blank_and_bad_version(self):
        with pytest.raises(ValidationError):
            DocumentPatch(content="  ")
        with pytest.raises(ValidationError):
            DocumentPatch(content="x", expected_version=0)


def test_response_mapping_marks_old_result_as_not_current():
    from datetime import UTC, datetime

    from bson import ObjectId

    from app.repositories.documents import build_result, new_pipeline

    now = datetime.now(UTC)
    doc = {
        "_id": ObjectId(),
        "user_id": "u",
        "title": "t",
        "content": "new",
        "content_hash": "h2",
        "content_version": 2,
        "status": "processing",
        "pipeline": new_pipeline(2),
        "result": build_result(version=1, chash="h1", summary="old", tags=["old"], now=now),
        "created_at": now,
        "updated_at": now,
    }
    out = to_document_out(doc, DocumentOut)
    assert out.result is not None and out.result.is_current is False
    assert out.result_is_current is False
    assert out.result.content_version == 1


def test_backoff_is_exponential_capped_and_jittered():
    settings = make_settings(retry_backoff_base_seconds=1, retry_backoff_max_seconds=4)
    runner = PipelineRunner(
        repo=None, rate_limiter=None, cache=None, settings=settings, simulator=None, rng=random.Random(0)
    )  # type: ignore[arg-type]
    for attempt, cap in [(1, 1), (2, 2), (3, 4), (4, 4), (10, 4)]:
        delay = runner._backoff(attempt)
        assert cap / 2 <= delay <= cap


def test_settings_reject_lease_shorter_than_a_stage():
    with pytest.raises(ValidationError):
        make_settings(processing_max_seconds=20, lease_seconds=10)
