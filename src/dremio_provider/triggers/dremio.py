from __future__ import annotations

import asyncio
import time
from typing import Any, AsyncIterator

from dremio_provider.hooks.dremio import DremioHook, ReflectionRefreshStatus
from airflow.triggers.base import BaseTrigger, TriggerEvent


class ReflectionRefreshTrigger(BaseTrigger):
    """Trigger class for checking reflection refresh status."""

    def __init__(
        self, reflection_id: str, conn_id: str, timeout: float, check_interval: float
    ):
        super().__init__()
        self.reflection_id = reflection_id
        self.conn_id = conn_id
        self.timeout = timeout
        self.check_interval = check_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "dremio_provider.triggers.dremio.ReflectionRefreshTrigger",
            {
                "reflection_id": self.reflection_id,
                "conn_id": self.conn_id,
                "timeout": self.timeout,
                "check_interval": self.check_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        hook = DremioHook(dremio_conn_id=self.conn_id)
        try:
            while await self.is_still_running(hook):
                if self.timeout < time.time():
                    yield TriggerEvent(
                        {
                            "status": "error",
                            "message": f"Refresh for reflection {self.reflection_id} has not finished after "
                            f"{self.timeout} seconds.",
                            "reflection_id": self.reflection_id,
                        }
                    )
                await asyncio.sleep(self.check_interval)
            refresh_status = hook.get_reflection_status(self.reflection_id)
            if refresh_status in ReflectionRefreshStatus.SUCCESS_STATES.value:
                yield TriggerEvent(
                    {
                        "status": "success",
                        "message": f"Refresh for reflection {self.reflection_id} has completed successfully.",
                        "reflection_id": self.reflection_id,
                    }
                )

            elif refresh_status == ReflectionRefreshStatus.DISABLED.value:
                yield TriggerEvent(
                    {
                        "status": "disabled",
                        "message": f"Reflection {self.reflection_id} has been manually disabled.",
                        "reflection_id": self.reflection_id,
                    }
                )
            else:
                yield TriggerEvent(
                    {
                        "status": "error",
                        "message": f"Refresh for reflection {self.reflection_id} has failed with state {refresh_status}.",
                        "reflection_id": self.reflection_id,
                    }
                )

        except Exception:
            yield TriggerEvent(
                {
                    "status": "error",
                    "message": f"Refresh for reflection {self.reflection_id} has failed with state {refresh_status}.",
                    "reflection_id": self.reflection_id,
                }
            )

    async def is_still_running(self, hook: DremioHook) -> bool:
        """Check whether the submitted job is running."""
        job_run_status = hook.get_reflection_status(self.reflection_id)
        if not ReflectionRefreshStatus.is_terminal(job_run_status):
            return True
        return False
