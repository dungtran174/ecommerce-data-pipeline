"""Shared helpers; load generator dependencies only when requested."""

__all__ = ["get_connection", "execute_query", "execute_insert_many", "FakeDataGenerator"]


def __getattr__(name):
    if name == "FakeDataGenerator":
        from helpers.faker_generators import FakeDataGenerator
        return FakeDataGenerator
    if name in {"get_connection", "execute_query", "execute_insert_many"}:
        from helpers import db_helpers
        return getattr(db_helpers, name)
    raise AttributeError(name)
