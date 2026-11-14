"""Every hot query must be served by the index we claim in the README (no COLLSCAN, no in-memory sort)."""

import pytest

from app.db.mongo import DOCUMENTS
from app.domain import ACTIVE_STATUSES
from app.repositories.documents import ref_filter

pytestmark = pytest.mark.integration

_ACTIVE = [s.value for s in ACTIVE_STATUSES]


async def _winning_plan(container, find: dict) -> str:
    plan = await container.mongo.db.command("explain", {"find": DOCUMENTS, **find}, verbosity="queryPlanner")
    return str(plan["queryPlanner"]["winningPlan"])


@pytest.mark.parametrize(
    ("find", "index"),
    [
        ({"filter": ref_filter("cms-1"), "limit": 1}, "client_doc_ref_unique"),
        (
            {"filter": {"user_id": "u"}, "sort": {"created_at": -1, "_id": -1}, "limit": 20},
            "user_created_desc",
        ),
        (
            {
                "filter": {"user_id": "u", "status": "completed"},
                "sort": {"created_at": -1, "_id": -1},
                "limit": 20,
            },
            "user_status_created_desc",
        ),
        ({"filter": {"user_id": "u", "status": {"$in": _ACTIVE}}}, "user_status_created_desc"),
        (
            {"filter": {"result": {"$type": "object"}, "result.content_hash": "h"}, "limit": 1},
            "result_content_hash",
        ),
    ],
)
async def test_query_uses_expected_index(container, find, index):
    plan = await _winning_plan(container, find)
    assert "COLLSCAN" not in plan, plan
    assert index in plan, plan
    assert "'stage': 'SORT'" not in plan, f"in-memory sort: {plan}"
