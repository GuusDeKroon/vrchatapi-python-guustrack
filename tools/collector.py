"""VRChat world statistics collector.

This script logs into the VRChat API using credentials sourced from environment
variables and periodically stores world statistics in a SQLite database.

Environment variables:
    VRCHAT_USERNAME: VRChat username (required).
    VRCHAT_PASSWORD: VRChat password (required).
    VRCHAT_USER_AGENT: Optional custom User-Agent string for API requests.
    VRCHAT_2FA_CODE: Optional one-time 2FA code for authenticator-enabled accounts.
    VRCHAT_EMAIL_2FA_CODE: Optional email 2FA code for email-only two-factor accounts.
    VRCHAT_DATABASE_PATH: Optional path to the SQLite database file. Defaults to
        ``data/world_stats.sqlite3``.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

import vrchatapi
from vrchatapi.api import authentication_api, worlds_api
from vrchatapi.exceptions import ApiException, UnauthorizedException
from vrchatapi.models.two_factor_auth_code import TwoFactorAuthCode
from vrchatapi.models.two_factor_email_code import TwoFactorEmailCode

WORLD_ID = "wrld_6a1f00c0-5eda-4a60-8876-f51b46c075cb"
DEFAULT_DATABASE_PATH = Path("data/world_stats.sqlite3")
COLLECTION_INTERVAL_SECONDS = 60 * 60
MIN_RETRY_DELAY_SECONDS = 60
MAX_RETRY_DELAY_SECONDS = 15 * 60
MAX_FETCH_ATTEMPTS = 5


def load_credentials() -> tuple[str, str]:
    """Load the VRChat credentials from environment variables."""

    required = {"VRCHAT_USERNAME", "VRCHAT_PASSWORD"}
    missing = [name for name in required if name not in os.environ]
    if missing:  # pragma: no cover - simple configuration guard
        raise RuntimeError(
            "Missing required environment variables: " + ", ".join(sorted(missing))
        )

    username = os.environ["VRCHAT_USERNAME"]
    password = os.environ["VRCHAT_PASSWORD"]
    return username, password


def determine_database_path() -> Path:
    env_path = os.getenv("VRCHAT_DATABASE_PATH")
    return Path(env_path) if env_path else DEFAULT_DATABASE_PATH


def ensure_schema(connection: sqlite3.Connection) -> None:
    """Create the database schema if it does not yet exist."""

    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS world_stats (
            timestamp TEXT NOT NULL,
            favorites INTEGER,
            visits INTEGER
        )
        """
    )
    connection.commit()


def persist_world_stats(
    connection: sqlite3.Connection,
    timestamp: datetime,
    favorites: Optional[int],
    visits: Optional[int],
) -> None:
    """Persist a world statistics snapshot to SQLite."""

    connection.execute(
        "INSERT INTO world_stats(timestamp, favorites, visits) VALUES (?, ?, ?)",
        (timestamp.isoformat(), favorites, visits),
    )
    connection.commit()


@contextmanager
def managed_database_connection(path: Path) -> Generator[sqlite3.Connection, None, None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    try:
        yield connection
    finally:
        connection.close()


def authenticate(api_client: vrchatapi.ApiClient) -> None:
    """Authenticate to the VRChat API handling optional 2FA challenges."""

    auth_api = authentication_api.AuthenticationApi(api_client)
    try:
        auth_api.get_current_user()
        return
    except UnauthorizedException as exc:
        if exc.status != 200:
            raise

        reason = exc.reason or ""
        if "Email 2 Factor Authentication" in reason:
            email_code = os.getenv("VRCHAT_EMAIL_2FA_CODE")
            if not email_code:
                raise RuntimeError(
                    "Email 2FA code required but VRCHAT_EMAIL_2FA_CODE is not set"
                ) from exc
            auth_api.verify2_fa_email_code(
                two_factor_email_code=TwoFactorEmailCode(code=email_code)
            )
        elif "2 Factor Authentication" in reason:
            two_factor_code = os.getenv("VRCHAT_2FA_CODE")
            if not two_factor_code:
                raise RuntimeError(
                    "Authenticator 2FA code required but VRCHAT_2FA_CODE is not set"
                ) from exc
            auth_api.verify2_fa(two_factor_auth_code=TwoFactorAuthCode(code=two_factor_code))
        else:
            raise

    auth_api.get_current_user()


def fetch_world(worlds_api_client: worlds_api.WorldsApi):
    """Fetch the configured world with retry/backoff."""

    delay = MIN_RETRY_DELAY_SECONDS
    last_error: Optional[Exception] = None

    for attempt in range(1, MAX_FETCH_ATTEMPTS + 1):
        try:
            return worlds_api_client.get_world(WORLD_ID)
        except ApiException as exc:
            last_error = exc
            logging.warning(
                "World fetch failed with status %s (attempt %s/%s)",
                getattr(exc, "status", "unknown"),
                attempt,
                MAX_FETCH_ATTEMPTS,
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            last_error = exc
            logging.exception("Unexpected error fetching world stats (attempt %s)", attempt)

        if attempt == MAX_FETCH_ATTEMPTS:
            break

        time.sleep(delay)
        delay = min(delay * 2, MAX_RETRY_DELAY_SECONDS)

    assert last_error is not None  # for mypy - we only exit loop on failure
    raise last_error


def collect_once(
    worlds_api_client: worlds_api.WorldsApi,
    connection: sqlite3.Connection,
) -> None:
    world = fetch_world(worlds_api_client)
    timestamp = datetime.now(timezone.utc)

    favorites = getattr(world, "favorites", None)
    visits = getattr(world, "visits", None)

    logging.info(
        "Fetched world stats at %s - favorites: %s visits: %s",
        timestamp.isoformat(),
        favorites,
        visits,
    )

    persist_world_stats(connection, timestamp, favorites, visits)


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def main() -> int:
    configure_logging()

    username, password = load_credentials()
    database_path = determine_database_path()

    configuration = vrchatapi.Configuration(username=username, password=password)

    user_agent = os.getenv(
        "VRCHAT_USER_AGENT",
        "GuusTrackCollector/1.0 (https://github.com/vrchatapi/vrchatapi-python)",
    )

    with managed_database_connection(database_path) as connection:
        ensure_schema(connection)

        with vrchatapi.ApiClient(configuration) as api_client:
            api_client.user_agent = user_agent
            logging.info("Authenticating with VRChat API as %s", username)
            authenticate(api_client)
            logging.info("Authentication succeeded")

            worlds_api_client = worlds_api.WorldsApi(api_client)

            while True:
                loop_started = time.monotonic()
                success = False
                try:
                    collect_once(worlds_api_client, connection)
                    success = True
                except ApiException as exc:
                    logging.exception("Failed to collect world stats: %s", exc)
                    if getattr(exc, "status", None) == 401:
                        logging.info("Refreshing authentication after unauthorized response")
                        authenticate(api_client)
                        logging.info("Authentication refreshed")
                except Exception as exc:  # pragma: no cover - defensive
                    logging.exception("Failed to collect world stats: %s", exc)

                elapsed = time.monotonic() - loop_started
                if success:
                    sleep_for = max(0, COLLECTION_INTERVAL_SECONDS - elapsed)
                else:
                    sleep_for = MIN_RETRY_DELAY_SECONDS

                if sleep_for:
                    logging.info("Sleeping for %.2f seconds", sleep_for)
                    time.sleep(sleep_for)

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:  # pragma: no cover - graceful shutdown
        print()
        logging.info("Collector interrupted by user")
        sys.exit(0)
