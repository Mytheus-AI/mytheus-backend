"""`check_grant_change`: the whole `/rbac/grants` write policy, as a pure function.

No database. Every rule in `services/identity_tenant_service/routes/rbac.py`'s docstring has a
case here, plus the permitted counterpart that proves the rule is not simply "deny everything".
"""

from __future__ import annotations

import uuid

import pytest

from libs.shared.errors import ErrorCode, ProblemDetail
from services.identity_tenant_service.routes.rbac import check_grant_change

CALLER = uuid.uuid4()
TARGET = uuid.uuid4()


def _check(
    caller_tier: str,
    current: str | None,
    new: str | None,
    *,
    owners: int = 2,
    target: uuid.UUID = TARGET,
) -> None:
    check_grant_change(
        caller_tier=caller_tier,
        caller_user_id=CALLER,
        target_user_id=target,
        current_tier=current,
        new_tier=new,
        owner_count=owners,
    )


def _denied(caller_tier: str, current: str | None, new: str | None, **kw: object) -> ProblemDetail:
    with pytest.raises(ProblemDetail) as info:
        _check(caller_tier, current, new, **kw)  # type: ignore[arg-type]
    return info.value


@pytest.mark.parametrize("tier", ["", "member"])
def test_below_admin_is_denied_even_without_the_route_gate(tier: str) -> None:
    err = _denied(tier, None, "member")
    assert (err.status, err.code) == (403, ErrorCode.FORBIDDEN_ROLE)


def test_admin_cannot_grant_owner() -> None:
    err = _denied("admin", "member", "owner")
    assert err.status == 403


@pytest.mark.parametrize("new", ["member", "admin"])
def test_admin_can_grant_up_to_admin_to_someone_below(new: str) -> None:
    _check("admin", None, new)
    _check("admin", "member", new)


@pytest.mark.parametrize("current", ["admin", "owner"])
def test_admin_cannot_touch_a_peer_or_superior(current: str) -> None:
    assert _denied("admin", current, "member").status == 403
    assert _denied("admin", current, None).status == 403


def test_admin_can_revoke_a_member() -> None:
    _check("admin", "member", None)


def test_nobody_changes_their_own_grant() -> None:
    for tier, new in [("owner", "admin"), ("owner", None), ("admin", "member")]:
        err = _denied(tier, tier, new, target=CALLER)
        assert err.status == 403


def test_owner_can_grant_owner_and_manage_other_owners() -> None:
    _check("owner", "member", "owner")
    _check("owner", "owner", "admin", owners=2)
    _check("owner", "owner", None, owners=2)


@pytest.mark.parametrize("new", ["admin", "member", None])
def test_last_owner_cannot_be_demoted_or_revoked(new: str | None) -> None:
    err = _denied("owner", "owner", new, owners=1)
    assert (err.status, err.code) == (409, ErrorCode.VERSION_CONFLICT)


def test_re_setting_owner_on_the_last_owner_is_not_a_demotion() -> None:
    _check("owner", "owner", "owner", owners=1)
