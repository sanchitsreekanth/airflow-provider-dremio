from __future__ import annotations

import json
from unittest.mock import patch

import pytest
from requests import Response

from airflow.models.connection import Connection
from dremio_provider.hooks.dremio import (
    DremioException,
    DremioHook,
    ReflectionRefreshStatus,
    TokenAuth,
)
from airflow.utils import db


def get_dremio_connection(
    conn_id: str = "dremio_default",
    conn_type: str = "http",
    host: str | None = None,
    port: int | None = None,
    extra: str | dict | None = None,
    login: str | None = None,
    password: str | None = None,
):
    if extra and not isinstance(extra, str):
        extra = json.dumps(extra)
    return Connection(
        conn_id=conn_id,
        host=host,
        port=port,
        login=login,
        password=password,
        extra=extra,
        conn_type=conn_type,
    )


def get_mock_response(code: int, response_body: dict | None = None):
    if response_body is None:
        response_body = {}
    response = Response()
    response._content = json.dumps(response_body).encode("utf-8")
    response.status_code = code
    return response


DREMIO_CONN_ID = "dremio_default"
DREMIO_CONN_ID_WITH_PORT = "dremio_conn_with_port"
DREMIO_CONN_ID_WITH_LOGIN = "dremio_conn_with_login"
DREMIO_CONN_ID_WITH_AUTH_TOKEN = "dremio_conn_with_auth_token"
DREMIO_CONN_ID_WITH_INVALID_AUTH_TOKEN = "dremio_conn_with_invalid_auth_token"
DREMIO_CONN_ID_WITH_PAT = "dremio_conn_with_pat"
DREMIO_CONN_ID_WITH_PAT_INVALID = "dremio_conn_with_pat_invalid"

DREMIO_CONNECTION = get_dremio_connection(host="test:9047", conn_id=DREMIO_CONN_ID)
DREMIO_CONNECTION_WITH_PORT = get_dremio_connection(
    conn_id=DREMIO_CONN_ID_WITH_PORT, host="test", port=9047
)
DREMIO_CONNECTION_WITH_LOGIN = get_dremio_connection(
    conn_id=DREMIO_CONN_ID_WITH_LOGIN,
    host="test:9047",
    login="username",
    password="pass",
)
DREMIO_CONNECTION_WITH_AUTH_TOKEN = get_dremio_connection(
    conn_id=DREMIO_CONN_ID_WITH_AUTH_TOKEN,
    host="test:9047",
    login="username",
    password="pass",
    extra={"auth": "AuthToken"},
)
DREMIO_CONNECTION_WITH_INVALID_AUTH_TOKEN = get_dremio_connection(
    conn_id=DREMIO_CONN_ID_WITH_INVALID_AUTH_TOKEN,
    host="test:9047",
    extra={"auth": "AuthToken"},
)
DREMIO_CONNECTION_WITH_PAT = get_dremio_connection(
    conn_id=DREMIO_CONN_ID_WITH_PAT,
    host="test:9047",
    login="username",
    password="pass",
    extra={"pat": "samplepattoken123", "auth": "PAT"},
)
DREMIO_CONNECTION_WITH_PAT_INVALID = get_dremio_connection(
    conn_id=DREMIO_CONN_ID_WITH_PAT_INVALID,
    host="test:9047",
    login="username",
    password="pass",
    extra={"auth": "PAT"},
)


class TestReflectionRefreshStates:
    valid_states = [
        "CAN_ACCELERATE",
        "CAN_ACCELERATE_WITH_FAILURES",
        "CANNOT_ACCELERATE_MANUAL",
        "CANNOT_ACCELERATE_SCHEDULED",
        "DISABLED",
        "EXPIRED",
        "FAILED",
        "INVALID",
        "INCOMPLETE",
        "REFRESHING",
        {
            "CAN_ACCELERATE",
            "CAN_ACCELERATE_WITH_FAILURES",
            "CANNOT_ACCELERATE_MANUAL",
            "CANNOT_ACCELERATE_SCHEDULED",
            "DISABLED",
            "EXPIRED",
            "FAILED",
            "INVALID",
        },
        ["INCOMPLETE", "REFRESHING"],
        [
            "CAN_ACCELERATE",
            "CAN_ACCELERATE_WITH_FAILURES",
            "CANNOT_ACCELERATE_MANUAL",
            "CANNOT_ACCELERATE_SCHEDULED",
            "DISABLED",
            "EXPIRED",
            "FAILED",
            "INVALID",
        ],
    ]

    invalid_states = [
        "ERROR",  # One invalid state
        ["ERROR", "SUCCESS"],  # Multiple invalid states
        ["Error", "EXPIRED", "DISABLED", "REFRESHING"],  # Some invalid states
        1,
        [1, 2],
    ]

    @pytest.mark.parametrize(argnames="states", argvalues=valid_states)
    def test_valid_ids_check(self, states):
        ReflectionRefreshStatus.validate(states)

    @pytest.mark.parametrize(argnames="states", argvalues=invalid_states)
    def test_invalid_job_run_status(self, states):
        with pytest.raises(ValueError):
            ReflectionRefreshStatus.validate(states)


class TestDremioHook:
    def setup_method(self):
        for conn in [
            DREMIO_CONNECTION,
            DREMIO_CONNECTION_WITH_PORT,
            DREMIO_CONNECTION_WITH_PAT,
            DREMIO_CONNECTION_WITH_AUTH_TOKEN,
            DREMIO_CONNECTION_WITH_LOGIN,
            DREMIO_CONNECTION_WITH_INVALID_AUTH_TOKEN,
            DREMIO_CONNECTION_WITH_PAT_INVALID,
        ]:
            db.merge_conn(conn)

    @pytest.mark.parametrize(
        argnames="conn_id",
        argvalues=[
            DREMIO_CONN_ID,
            DREMIO_CONN_ID_WITH_PORT,
            DREMIO_CONN_ID_WITH_LOGIN,
            DREMIO_CONN_ID_WITH_AUTH_TOKEN,
            DREMIO_CONN_ID_WITH_PAT,
        ],
    )
    @patch("requests.post")
    def test_hook_init(self, post_mock, conn_id: str):
        post_mock.return_value = get_mock_response(
            code=200, response_body={"token": "somerandomtoken"}
        )
        hook = DremioHook(dremio_conn_id=conn_id)
        assert hook.auth_type == TokenAuth
        assert hook.dremio_url == "http://test:9047"
        session = hook.get_conn()
        assert isinstance(session.auth, TokenAuth)

    @pytest.mark.parametrize(
        argnames="conn_id", argvalues=[DREMIO_CONN_ID_WITH_AUTH_TOKEN]
    )
    @patch("requests.post")
    def test_hook_gets_auth_token(self, post_mock, conn_id):
        post_mock.return_value = get_mock_response(
            code=200, response_body={"token": "somerandomtoken"}
        )
        hook = DremioHook(dremio_conn_id=conn_id)
        assert hook.auth_type == TokenAuth
        session = hook.get_conn()
        assert session.auth.token == "_dremiosomerandomtoken"
        post_mock.assert_called_with(
            url="http://test:9047/apiv2/login",
            headers={"Content-Type": "application/json"},
            data='{"userName": "username", "password": "pass"}',
        )

    @pytest.mark.parametrize(
        argnames="conn_id", argvalues=[DREMIO_CONN_ID_WITH_INVALID_AUTH_TOKEN]
    )
    @patch("requests.post")
    def test_hook_raises_exception_for_invalid_connection(self, post_mock, conn_id):
        post_mock.return_value = get_mock_response(
            code=400, response_body={"reason": "Bad Request"}
        )
        hook = DremioHook(dremio_conn_id=conn_id)
        with pytest.raises(ValueError):
            hook.get_conn()

    @pytest.mark.parametrize(argnames="conn_id", argvalues=[DREMIO_CONN_ID_WITH_PAT])
    def test_hook_gets_pat_token(self, conn_id):
        hook = DremioHook(dremio_conn_id=conn_id)
        assert hook.auth_type == TokenAuth
        session = hook.get_conn()
        assert session.auth.token == "samplepattoken123"

    @pytest.mark.parametrize(
        argnames="conn_id", argvalues=[DREMIO_CONN_ID_WITH_PAT_INVALID]
    )
    def test_hook_raises_exception_for_invalid_pat_connection(self, conn_id):
        hook = DremioHook(dremio_conn_id=conn_id)
        with pytest.raises(AttributeError):
            hook.get_conn()

    @patch("airflow.providers.http.hooks.http.HttpHook.run")
    def test_execute_sql_query(self, run_mock):
        run_mock.return_value = get_mock_response(code=200, response_body={"id": 123})
        hook = DremioHook(dremio_conn_id=DREMIO_CONN_ID)
        hook.execute_sql_query(sql="SELECT * FROM TEST")
        run_mock.assert_called_with(
            endpoint="sql", data={"sql": "SELECT * FROM TEST"}, headers={}
        )
        assert hook.method == "POST"

    @patch("airflow.providers.http.hooks.http.HttpHook.run")
    def test_get_catalog_by_path(self, run_mock):
        run_mock.return_value = get_mock_response(
            code=200, response_body={"datasetId": 123}
        )
        hook = DremioHook(dremio_conn_id=DREMIO_CONN_ID)
        hook.get_catalog_by_path(path="source/schema/table")
        run_mock.assert_called_with(
            endpoint="catalog/by-path/source/schema/table", data={}, headers={}
        )
        assert hook.method == "GET"

    @patch("airflow.providers.http.hooks.http.HttpHook.run")
    def test_get_reflection_status(self, run_mock):
        run_mock.return_value = get_mock_response(
            code=200, response_body={"status": {"combinedStatus": "CAN_ACCELERATE"}}
        )
        hook = DremioHook(dremio_conn_id=DREMIO_CONN_ID)
        status = hook.get_reflection_status(reflection_id="12345")
        assert status == "CAN_ACCELERATE"
        run_mock.assert_called_with(endpoint="reflection/12345", data={}, headers={})
        assert hook.method == "GET"

    @patch("airflow.providers.http.hooks.http.HttpHook.run")
    def test_refresh_table_metadata(self, run_mock):
        run_mock.return_value = get_mock_response(
            code=200, response_body={"id": "111111"}
        )
        hook = DremioHook(dremio_conn_id=DREMIO_CONN_ID)
        job_id = hook.refresh_table_metadata(table="schema.table")
        assert job_id == "111111"
        run_mock.assert_called_with(
            endpoint="sql",
            data={"sql": "ALTER TABLE schema.table REFRESH METADATA"},
            headers={},
        )
        assert hook.method == "POST"

    @patch("airflow.providers.http.hooks.http.HttpHook.run")
    def test_wait_for_completion(self, run_mock):
        run_mock.side_effect = [
            get_mock_response(
                code=200, response_body={"status": {"combinedStatus": "REFRESHING"}}
            ),
            get_mock_response(
                code=200, response_body={"status": {"combinedStatus": "CAN_ACCELERATE"}}
            ),
        ]
        hook = DremioHook(dremio_conn_id=DREMIO_CONN_ID)
        complete = hook.wait_for_reflection_completion(
            reflection_id="123", check_interval=5
        )
        assert complete
        assert run_mock.call_count == 2

    @patch("airflow.providers.http.hooks.http.HttpHook.run")
    def test_wait_for_completion_raises_exception_on_timeout(self, run_mock):
        run_mock.side_effect = [
            get_mock_response(
                code=200, response_body={"status": {"combinedStatus": "REFRESHING"}}
            ),
            get_mock_response(
                code=200, response_body={"status": {"combinedStatus": "REFRESHING"}}
            ),
            get_mock_response(
                code=200, response_body={"status": {"combinedStatus": "CAN_ACCELERATE"}}
            ),
        ]
        hook = DremioHook(dremio_conn_id=DREMIO_CONN_ID)
        with pytest.raises(DremioException):
            hook.wait_for_reflection_completion(
                reflection_id="123", check_interval=5, timeout=4
            )
