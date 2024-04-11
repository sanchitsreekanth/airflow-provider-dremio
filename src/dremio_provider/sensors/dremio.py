from __future__ import annotations

from functools import cached_property

from airflow.sensors.base import BaseSensorOperator, PokeReturnValue
from airflow.utils.context import Context

from dremio_provider.hooks.dremio import DremioHook, JobRunStatus, DremioException


class DremioJobSensor(BaseSensorOperator):
    template_fields = ("job_id",)

    def __init__(
        self,
        job_id: str,
        dremio_conn_id: str,
    ):
        self.job_id = job_id
        self.dremio_conn_id = dremio_conn_id

    @cached_property
    def hook(self) -> DremioHook:
        return DremioHook(dremio_conn_id=self.dremio_conn_id)

    def poke(self, context: Context) -> bool | PokeReturnValue:
        job_details = self.hook.get_job_results(job_id=self.job_id)
        job_status = job_details.get("jobState")

        if job_details in JobRunStatus.FAILED_STATES.value:
            self.log.error("Job has failed with state %s", job_status)
            err_message = job_details.get("errorMessage")
            self.log.error(err_message)
            raise DremioException(err_message)

        if job_details in JobRunStatus.SUCCESS_STATES.value:
            self.log.info("Job has succeeded with state %s", job_status)
            return True

        else:
            self.log.info("Job has not completed. Current state is %s", job_status)
            return False
