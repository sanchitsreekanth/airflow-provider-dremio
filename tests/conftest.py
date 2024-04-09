import pytest


@pytest.fixture(scope="module", autouse=True)
def reset_db():
    """Resets Airflow db."""

    from airflow.utils import db

    db.resetdb()
