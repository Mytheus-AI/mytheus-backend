"""Root pytest conftest — shared fixtures for all services.

testcontainers spins up real Postgres, Redis, and Redpanda per test session,
so integration tests hit real engines (RLS policies, outbox/inbox, Avro round-trips).
"""

import pytest
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer


@pytest.fixture(scope="session")
def postgres_container():
    with PostgresContainer("supabase/postgres:15.8.1.060") as pg:
        yield pg


@pytest.fixture(scope="session")
def redis_container():
    with RedisContainer("redis:7-alpine") as redis:
        yield redis


@pytest.fixture
def postgres_url(postgres_container) -> str:
    return postgres_container.get_connection_url().replace("psycopg2", "asyncpg")


@pytest.fixture
def redis_url(redis_container) -> str:
    return redis_container.get_connection_url()
