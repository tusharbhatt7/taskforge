"""DATABASE_URL normalization.

Managed providers hand out libpq-style URLs. asyncpg rejects libpq-only query parameters
as unexpected connect() kwargs, so pasting a Neon/Supabase/Heroku URL verbatim would
otherwise crash at startup. These cases pin the translation.
"""

import pytest

from app.core.config import _normalize


@pytest.mark.parametrize("raw", [
    "postgres://u:p@host/db",
    "postgresql://u:p@host/db",
    "postgresql+asyncpg://u:p@host/db",
])
def test_scheme_is_normalized_to_asyncpg(raw):
    url, args = _normalize(raw)
    assert url == "postgresql+asyncpg://u:p@host/db"
    assert args == {}


def test_neon_style_url_translates_sslmode_into_a_connect_arg():
    url, args = _normalize(
        "postgresql://user:pw@ep-cool-name-123.ap-southeast-1.aws.neon.tech/neondb?sslmode=require")
    assert url == "postgresql+asyncpg://user:pw@ep-cool-name-123.ap-southeast-1.aws.neon.tech/neondb"
    assert args == {"ssl": True}, "TLS must be requested through connect_args, not the URL"


def test_channel_binding_and_other_libpq_params_are_dropped():
    url, args = _normalize(
        "postgresql://u:p@host/db?sslmode=require&channel_binding=require"
        "&connect_timeout=10&application_name=taskforge")
    assert "channel_binding" not in url
    assert "connect_timeout" not in url
    assert "application_name" not in url
    assert args == {"ssl": True}


def test_sslmode_disable_does_not_request_tls():
    _, args = _normalize("postgresql://u:p@host/db?sslmode=disable")
    assert args == {}


def test_unknown_params_are_preserved():
    """Only known-incompatible parameters are stripped; anything else is left alone."""
    url, _ = _normalize("postgresql://u:p@host/db?prepared_statement_cache_size=0")
    assert "prepared_statement_cache_size=0" in url


def test_password_with_special_characters_survives():
    url, _ = _normalize("postgresql://u:p%40ss%3Aword@host:5432/db?sslmode=require")
    assert "p%40ss%3Aword" in url, "percent-encoding must not be mangled"
    assert url.endswith("/db")


def test_local_dev_url_is_untouched():
    url, args = _normalize("postgresql+asyncpg://taskforge:taskforge@localhost:5432/taskforge")
    assert url == "postgresql+asyncpg://taskforge:taskforge@localhost:5432/taskforge"
    assert args == {}
