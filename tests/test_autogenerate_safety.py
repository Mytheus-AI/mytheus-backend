"""Autogenerate must never propose destroying a table this repo does not model.

The defect this guards: `migrations/env.py` passed no `include_object`, and the Makefile's
`migrate` target ran `revision --autogenerate` followed immediately by `upgrade head`. Four
tables in this database have no SQLAlchemy model — `meeting_rooms`, `meeting_speakers`,
`chat_messages`, and `mytheus-realtime`'s `transcript_chunks` — plus every declarative
partition. Autogenerate reads "not in metadata" as "drop it", so adding a single column could
generate and apply a revision that destroyed all of them, unreviewed, in one command.

`866f7eedb0e7`'s docstring records a developer catching exactly this and hand-writing the
migration body instead. That worked once, for one person, on one migration. This test is the
version that keeps working.

Asserting on `compare_metadata` rather than on generated files: the question is whether the
comparison itself yields a removal, which is the thing that would end up in a revision. Going
through the file would test the same property with more moving parts.
"""

from __future__ import annotations

import psycopg2
import pytest
import sqlalchemy as sa
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext

from tests.conftest import _SYNC_URL

# Diff verbs that destroy something. `remove_table` motivated this test; the others are here
# because dropping a table's index, constraint or column is a smaller version of the same
# mistake and reads identically in a generated file.
_DESTRUCTIVE = {
    "remove_table",
    "remove_index",
    "remove_constraint",
    "remove_column",
}

# `remove_table_comment` is deliberately NOT in that set. Several migrations write long
# COMMENT ON TABLE strings as documentation (a1c4e7f20b18:122 on `organizations`,
# c3e8b1d47f92:148 on `audit_logs`) and the models do not restate them, so autogenerate
# proposes clearing them. That deletes a sentence, not a row. Mirroring the text into
# `__table_args__` would duplicate it in two places and invite the two copies to drift, which
# is a worse outcome than a comment this test declines to police. Recorded here rather than
# filtered silently, so the next person knows it was a decision.


def _database_reachable() -> bool:
    try:
        psycopg2.connect(_SYNC_URL).close()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _database_reachable(),
    reason=f"needs the migrated test Postgres at {_SYNC_URL}",
)


def _flatten(diffs: list) -> list[tuple]:
    """`compare_metadata` nests per-table column diffs one list deep."""
    out: list[tuple] = []
    for d in diffs:
        if isinstance(d, list):
            out.extend(x for x in d if isinstance(x, tuple))
        elif isinstance(d, tuple):
            out.append(d)
    return out


def _target_metadata() -> sa.MetaData:
    """The same metadata env.py builds, assembled the same way.

    env.py cannot be imported — it runs migrations as an import side effect — so the model
    modules are imported here directly. `migrations/autogen_filter.py` exists so the rules
    under test are literally the ones env.py installs, rather than a copy that can drift.
    """
    import libs.shared.infra_models  # noqa: F401
    import services.admin_ops_service.models  # noqa: F401
    import services.core_api.models  # noqa: F401
    import services.identity_tenant_service.models  # noqa: F401
    from libs.shared.db import Base

    return Base.metadata


def _diff_against_live_database() -> list[tuple]:
    from migrations.autogen_filter import include_object

    target_metadata = _target_metadata()

    engine = sa.create_engine(_SYNC_URL, poolclass=sa.pool.NullPool)
    with engine.connect() as connection:
        context = MigrationContext.configure(
            connection,
            opts={"include_object": include_object, "compare_type": False},
        )
        return _flatten(compare_metadata(context, target_metadata))


def test_autogenerate_proposes_no_destructive_operation() -> None:
    """The whole point. A clean checkout against a migrated database drops nothing."""
    destructive = [d for d in _diff_against_live_database() if d[0] in _DESTRUCTIVE]
    assert (
        destructive == []
    ), "autogenerate would DESTROY schema objects on the next `make revision`:\n  " + "\n  ".join(
        f"{d[0]}: {getattr(d[1], 'name', d[1])}" for d in destructive
    )


@pytest.mark.parametrize(
    "table",
    ["meeting_rooms", "meeting_speakers", "chat_messages", "transcript_chunks"],
)
def test_unmodelled_table_is_not_proposed_for_removal(table: str) -> None:
    """Named individually so a failure says which table, not just that something broke.

    `transcript_chunks` is the one that matters most: this repo neither owns nor migrates it.
    """
    removals = {
        getattr(d[1], "name", None) for d in _diff_against_live_database() if d[0] == "remove_table"
    }
    assert table not in removals


def test_partitions_are_not_proposed_for_removal() -> None:
    """Partitions are never in metadata and the set grows monthly, so they are matched by
    shape rather than by a list that would go stale exactly when it starts mattering."""
    removals = {
        getattr(d[1], "name", "") for d in _diff_against_live_database() if d[0] == "remove_table"
    }
    partitions = {n for n in removals if n.startswith(("audit_logs_", "chat_messages_"))}
    assert partitions == set(), f"partitions proposed for removal: {sorted(partitions)}"


def test_filter_still_allows_additions() -> None:
    """A filter that suppressed everything would pass every test above and be useless.

    A table present in metadata and absent from the database must still be proposed for
    creation, so `include_object` is verified to block only the destructive direction.
    """
    from migrations.autogen_filter import include_object

    unknown = sa.Table("a_table_that_does_not_exist", sa.MetaData(), sa.Column("id", sa.Integer))
    assert include_object(unknown, unknown.name, "table", False, None) is True
