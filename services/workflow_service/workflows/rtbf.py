"""RTBFWorkflow — Right-to-be-forgotten deletion saga.

Triggered by admin-events (erasure_requested).
Deletes a tenant/user across all six stores: Postgres, Qdrant, AGE, Redis, object storage, audit.
Keyed by org_id / user_id so Temporal can deduplicate concurrent requests.
"""

from temporalio import workflow


@workflow.defn
class RTBFWorkflow:
    @workflow.run
    async def run(self, org_id: str, user_id: str | None = None) -> None:
        # TODO: activities for each store:
        # 1. Postgres soft-delete then hard purge by org_id/user_id
        # 2. Qdrant delete points by org_id filter
        # 3. AGE tag + remove nodes/edges
        # 4. Redis flush nexus:{org_id}:* keys
        # 5. Object storage delete {org_id}/* prefixes
        # 6. audit-service: append RTBF completion record
        pass
