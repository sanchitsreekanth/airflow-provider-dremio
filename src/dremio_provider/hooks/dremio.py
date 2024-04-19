from __future__ import annotations

import json
import time
from enum import Enum
from functools import cached_property
from json import JSONDecodeError
from typing import Any, Sequence, Set
from urllib.parse import urljoin

import requests
from requests.auth import AuthBase
from requests.models import DEFAULT_REDIRECT_LIMIT, PreparedRequest

from airflow.providers.http.hooks.http import HttpHook


class DremioException(Exception):
    """Dremio Exception class."""


def _jsonify(obj: Any) -> dict:
    """Return a JSON serializable object from obj."""
    if not obj:
        return {}

    if isinstance(obj, str):
        try:
            json_val = json.loads(obj)
            return json_val
        except JSONDecodeError:
            raise DremioException(f"Invalid json string provided - {obj}")
    elif isinstance(obj, dict):
        return obj
    else:
        return {}


def _get_provider_info() -> tuple[str, str]:
    from airflow.providers_manager import ProvidersManager

    manager = ProvidersManager()
    package_name = manager.hooks[DremioHook.conn_type].package_name  # type: ignore[union-attr]
    provider = manager.providers[package_name]

    return package_name, provider.version


class TokenAuth(AuthBase):
    """Class for token based auth for dremio."""

    def __init__(self, token: str):
        self.token = token

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        package_name, provider_version = _get_provider_info()
        request.headers["User-Agent"] = f"{package_name}-v{provider_version}"
        request.headers["Content-Type"] = "application/json"
        request.headers["Authorization"] = self.token
        return request


class JobRunStatus(Enum):
    NOT_SUBMITTED = "NOT_SUBMITTED"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    CANCELED = "CANCELED"
    FAILED = "FAILED"
    CANCELLATION_REQUESTED = "CANCELLATION_REQUESTED"
    PLANNING = "PLANNING"
    PENDING = "PENDING"
    METADATA_RETRIEVAL = "METADATA_RETRIEVAL"
    QUEUED = "QUEUED"
    ENGINE_START = "ENGINE_START"
    EXECUTION_PLANNING = "EXECUTION_PLANNING"
    INVALID_STATE = "INVALID_STATE"

    TERMINAL_STATES = (COMPLETED, CANCELED, FAILED, INVALID_STATE)
    NON_TERMINAL_STATES = (
        NOT_SUBMITTED,
        STARTING,
        RUNNING,
        CANCELLATION_REQUESTED,
        PLANNING,
        PENDING,
        METADATA_RETRIEVAL,
        QUEUED,
        EXECUTION_PLANNING,
        ENGINE_START,
    )
    SUCCESS_STATES = (COMPLETED,)
    FAILED_STATES = (CANCELED, FAILED, INVALID_STATE)


class ReflectionRefreshStatus(Enum):
    """Reflection status class."""

    CAN_ACCELERATE = "CAN_ACCELERATE"
    CAN_ACCELERATE_WITH_FAILURES = "CAN_ACCELERATE_WITH_FAILURES"
    CANNOT_ACCELERATE_MANUAL = "CANNOT_ACCELERATE_MANUAL"
    CANNOT_ACCELERATE_SCHEDULED = "CANNOT_ACCELERATE_SCHEDULED"
    DISABLED = "DISABLED"
    EXPIRED = "EXPIRED"
    FAILED = "FAILED"
    INVALID = "INVALID"
    INCOMPLETE = "INCOMPLETE"
    REFRESHING = "REFRESHING"
    TERMINAL_STATES = (
        CAN_ACCELERATE,
        CAN_ACCELERATE_WITH_FAILURES,
        CANNOT_ACCELERATE_SCHEDULED,
        CANNOT_ACCELERATE_MANUAL,
        DISABLED,
        EXPIRED,
        FAILED,
        INVALID,
    )
    NON_TERMINAL_STATES = (INCOMPLETE, REFRESHING)
    SUCCESS_STATES = (CAN_ACCELERATE,)
    FAILURE_STATES = (
        CAN_ACCELERATE_WITH_FAILURES,
        CANNOT_ACCELERATE_SCHEDULED,
        CANNOT_ACCELERATE_MANUAL,
        DISABLED,
        EXPIRED,
        FAILED,
        INVALID,
    )

    @classmethod
    def validate(cls, states: str | Sequence[str] | set[str]):
        if isinstance(states, (Sequence, Set)) and not isinstance(states, str):
            for state in states:
                cls(state)
        else:
            cls(states)

    @classmethod
    def is_terminal(cls, state: str) -> bool:
        """Check if the input status is that of a terminal type."""
        cls.validate(states=state)

        return state in cls.TERMINAL_STATES.value


class DremioHook(HttpHook):
    """Interacts with Dremio Cluster using Dremio REST APIs."""

    conn_attr_name = "dremio_conn_id"
    default_conn_name = "dremio_default"
    conn_type = "http"
    hook_name = "Dremio"
    default_api_version = "api/v3"

    def __init__(
        self,
        dremio_conn_id: str = default_conn_name,
        headers: dict | None = None,
        api_version: str = default_api_version,
        *args,
        **kwargs,
    ) -> None:
        self.dremio_conn_id = dremio_conn_id
        self.api_version = api_version
        self.headers = headers or {"Content-Type": "application/json"}
        kwargs["auth_type"] = TokenAuth
        super().__init__(*args, **kwargs)

    @cached_property
    def token(self) -> str:
        conn = self.get_connection(conn_id=self.dremio_conn_id)
        extra = conn.extra_dejson if conn.extra else {}
        if not extra:
            return ""

        auth = extra.get("auth", None)
        pat = extra.get("pat", "")
        if not auth:
            self.log.warning("No authentication provided for Dremio")
            return ""

        if auth and auth not in ("AuthToken", "PAT"):
            raise ValueError(
                f"Received invalid auth type {auth} in connection extra. Please provide either 'AuthToken' or PAT' for authentication. Please refer https://docs.dremio.com/current/reference/api/#authentication for more details"
            )

        if auth == "AuthToken":
            if not all([conn.login, conn.password]):
                raise ValueError(
                    f"Both login and password must be set in {self.dremio_conn_id} connection for using AuthToken"
                )

            token = self.__fetch_auth_token(conn.login, conn.password)
            return f"_dremio{token}"

        elif auth == "PAT":
            if conn.extra and not pat:
                raise AttributeError(
                    "Auth method 'PAT' requires a value for key 'pat' in connection extra fields which contains value for the pat token"
                )

            return pat

        else:
            return ""

    @cached_property
    def dremio_url(self):
        conn = self.get_connection(self.dremio_conn_id)
        if conn.host and "://" in conn.host:
            base_url = conn.host
        else:
            # schema defaults to HTTP
            schema = conn.schema if conn.schema else "http"
            host = conn.host if conn.host else ""
            base_url = f"{schema}://{host}"

        if conn.port:
            base_url += f":{conn.port}"

        return base_url

    def __fetch_auth_token(self, username: str, password: str):
        token_url = urljoin(self.dremio_url, "apiv2/login")
        data = json.dumps({"userName": username, "password": password})
        headers = {"Content-Type": "application/json"}
        response = requests.post(url=token_url, data=data, headers=headers)
        self.check_response(response)
        return response.json().get("token")

    def get_conn(self, headers: dict[Any, Any] | None = None) -> requests.Session:
        session = requests.Session()
        if self.dremio_conn_id:
            conn = self.get_connection(self.dremio_conn_id)
            self.base_url = self.dremio_url

            session.auth = TokenAuth(token=self.token)
            if conn.extra:
                extra = conn.extra_dejson
                extra.pop(
                    "timeout", None
                )  # ignore this as timeout is only accepted in request method of Session
                extra.pop(
                    "allow_redirects", None
                )  # ignore this as only max_redirects is accepted in Session
                session.proxies = extra.pop("proxies", extra.pop("proxy", {}))
                session.stream = extra.pop("stream", False)
                session.verify = extra.pop("verify", extra.pop("verify_ssl", True))
                session.cert = extra.pop("cert", None)
                session.max_redirects = extra.pop(
                    "max_redirects", DEFAULT_REDIRECT_LIMIT
                )
        return session

    def run_and_get_response(
        self,
        method: str,
        endpoint: str,
        data: dict | str | None = None,
        headers: dict | str | None = None,
    ):
        data = json.dumps(_jsonify(data))
        headers = {**_jsonify(headers)}
        method = method.upper()
        self.method = method
        if endpoint.startswith("/"):
            endpoint = endpoint[1:]
        endpoint = f"{self.api_version}/{endpoint}"
        response = self.run(endpoint=endpoint, data=data, headers=headers)
        if response.status_code != 204:
            return response.json()

    def execute_sql_query(self, sql: str, **sql_kwargs):
        body = {**{"sql": sql, **sql_kwargs}}
        self.log.info("Executing SQL query %s", sql)
        response = self.run_and_get_response(method="POST", data=body, endpoint="sql")
        return response

    def get_catalog_by_path(self, path: str, **kwargs):
        return self.run_and_get_response(
            method="GET", data=kwargs, endpoint=f"catalog/by-path/{path}"
        )

    def get_catalog(self, catalog_id: str, **kwargs):
        return self.run_and_get_response(
            method="GET", data=kwargs, endpoint=f"catalog/{catalog_id}"
        )

    def get_reflection(self, reflection_id: str):
        return self.run_and_get_response(
            method="GET", endpoint=f"reflection/{reflection_id}"
        )

    def create_reflection(self, reflection_spec: dict):
        return self.run_and_get_response(
            method="POST", endpoint="reflection", data=reflection_spec
        )

    def update_reflection(self, reflection_id: str, spec: dict):
        return self.run_and_get_response(
            method="PUT", endpoint=f"reflection/{reflection_id}", data=spec
        )

    def get_dataset_reflection(self, dataset_id: str):
        return self.run_and_get_response(
            method="GET", endpoint=f"dataset/{dataset_id}/reflection"
        )

    def get_reflections_for_source(self, source: str):
        dataset_id = self.get_catalog_by_path(path=source.replace(".", "/")).get("id")
        response = self.get_dataset_reflection(dataset_id)
        return response.get("data")

    def get_job(self, job_id: str):
        return self.run_and_get_response(method="GET", endpoint=f"job/{job_id}")

    def get_job_results(self, job_id):
        return self.run_and_get_response(method="GET", endpoint=f"job/{job_id}/results")

    def refresh_table_metadata(self, table: str, context: list[str] | None = None):
        sql = f"ALTER TABLE {table} REFRESH METADATA"
        sql_kwargs = {"context": context} if context else {}
        job_id = self.execute_sql_query(sql=sql, **sql_kwargs).get("id")
        self.log.info("Job id for %s metadata refresh is %s", table, job_id)
        return job_id

    def trigger_reflection_refresh(self, dataset_id: str):
        return self.run_and_get_response(
            method="POST", endpoint=f"catalog/{dataset_id}/refresh"
        )

    def set_property(self, property_name: str, value: str, level: str = "system"):
        return self.execute_sql_query(
            sql=f"ALTER {level.upper()} SET {property_name} = {value}"
        )

    def unset_property(self, property_name: str, level: str = "system"):
        return self.execute_sql_query(
            sql=f"ALTER {level.upper()} RESET {property_name}"
        )

    def get_reflection_status(self, reflection_id: str) -> str:
        self.log.info("Getting the status of reflection refresh %s.", reflection_id)

        job_run = self.get_reflection(reflection_id)
        job_run_status = job_run["status"]["combinedStatus"]

        self.log.info(
            "Current status of reflection refresh %s: %s",
            reflection_id,
            ReflectionRefreshStatus(job_run_status).name,
        )

        return job_run_status

    def wait_for_reflection_completion(
        self,
        reflection_id: str,
        check_interval: int = 60,
        timeout: int = 60 * 60 * 24 * 7,
    ) -> bool:
        expected_states = ReflectionRefreshStatus.SUCCESS_STATES.value
        reflection_status = self.get_reflection_status(reflection_id)

        start_time = time.monotonic()

        while (
            not ReflectionRefreshStatus.is_terminal(reflection_status)
            and reflection_status not in expected_states
        ):
            # Check if the job-run duration has exceeded the ``timeout`` configured
            if start_time + timeout < time.monotonic():
                raise DremioException(
                    f"Reflection {reflection_id} has not completed after {timeout} seconds"
                )

            time.sleep(check_interval)
            reflection_status = self.get_reflection_status(reflection_id)

        return reflection_status in expected_states
