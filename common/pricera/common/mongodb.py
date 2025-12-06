from __future__ import annotations

from contextlib import contextmanager
from typing import Generator, Optional, Dict, Any

from pymongo import MongoClient
from pymongo.database import Database

from .utilities import get_env_value


def _build_mongodb_uri(
    uri: Optional[str] = None,
    *,
    host: Optional[str] = None,
    port: Optional[int] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    auth_db: Optional[str] = None,
    tls: Optional[bool] = None,
    replica_set: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Build a MongoDB URI from explicit arguments or environment variables.

    Default format (when no explicit URI is provided):
      mongodb+srv://{user}:{password}@{host}/?retryWrites=true&w=majority

    Environment variables supported (used when corresponding explicit args are not provided):
      - MONGODB_URI
      - MONGODB_HOST (default: cluster0.aqgji.mongodb.net)
      - MONGODB_USER
      - MONGODB_PASSWORD
      - MONGODB_AUTH_DB (optional path segment, typically not required for SRV)

    Explicit function arguments take precedence over environment variables.
    """
    # If a complete URI is provided, return it as-is
    if uri is None:
        uri = get_env_value("MONGODB_URI")
    if uri:
        return uri

    # SRV-style defaults
    host = host or get_env_value("MONGODB_HOST")
    user = user or get_env_value("MONGODB_USER")
    password = password or get_env_value("MONGODB_PASSWORD")

    # Optional path database (rarely needed with SRV, but keep for compatibility)
    auth_db = auth_db or get_env_value("MONGODB_AUTH_DB")
    path = f"/{auth_db}" if auth_db else "/"

    # Base SRV URI with retryWrites and majority write concern by default
    credentials = f"{user}:{password}@" if user and password else ""
    base = f"mongodb+srv://{credentials}{host}{path}"

    # Default query params for SRV
    query: Dict[str, Any] = {"retryWrites": "true", "w": "majority"}
    if params:
        query.update(params)

    q = "&".join(f"{k}={v}" for k, v in query.items())
    return f"{base}?{q}"


@contextmanager
def get_mongo_client(
    uri: Optional[str] = None,
    *,
    server_selection_timeout_ms: int = 10000,
    connect_timeout_ms: int = 10000,
    socket_timeout_ms: Optional[int] = None,
    kwargs: Optional[Dict[str, Any]] = None,
) -> Generator[MongoClient, None, None]:
    """
    Context manager that yields a configured MongoClient and guarantees proper cleanup.

    Usage:
        with mongo_client() as client:
            client.dbname.collection.find_one(...)

    Parameters may be provided directly or via environment variables recognized by _build_mongodb_uri.
    """
    final_uri = _build_mongodb_uri(uri)
    client_kwargs: Dict[str, Any] = dict(
        serverSelectionTimeoutMS=server_selection_timeout_ms,
        connectTimeoutMS=connect_timeout_ms,
    )
    if socket_timeout_ms is not None:
        client_kwargs["socketTimeoutMS"] = socket_timeout_ms
    if kwargs:
        client_kwargs.update(kwargs)

    client = MongoClient(final_uri, **client_kwargs)
    try:
        # Validate connectivity early; raises ServerSelectionTimeoutError if unreachable
        client.admin.command("ping")
        yield client
    finally:
        client.close()


@contextmanager
def mongo_db(
    db_name: str,
    *,
    uri: Optional[str] = None,
    client: Optional[MongoClient] = None,
    server_selection_timeout_ms: int = 10000,
    connect_timeout_ms: int = 10000,
    socket_timeout_ms: Optional[int] = None,
    kwargs: Optional[Dict[str, Any]] = None,
) -> Generator[Database, None, None]:
    """
    Context manager that yields a Database instance. If an external client is provided,
    it will not be closed on exit. If no client is provided, a temporary client will be
    created and automatically closed.

    Usage:
        with mongo_db("my_database") as db:
            db.my_collection.insert_one({"hello": "world"})

        # Or reuse an existing client
        with mongo_client() as c:
            with mongo_db("my_database", client=c) as db:
                db.my_collection.find_one({})
    """
    if client is not None:
        # Use provided client and do not manage its lifecycle
        db = client[db_name]
        # Attempt a quick command to validate database availability (optional)
        db.command("ping")
        try:
            yield db
        finally:
            # Do not close externally provided client
            pass
    else:
        with get_mongo_client(
            uri,
            server_selection_timeout_ms=server_selection_timeout_ms,
            connect_timeout_ms=connect_timeout_ms,
            socket_timeout_ms=socket_timeout_ms,
            kwargs=kwargs,
        ) as c:
            db = c[db_name]
            # Ensure db is reachable
            db.command("ping")
            yield db


__all__ = [
    "get_mongo_client",
    "mongo_db",
]
