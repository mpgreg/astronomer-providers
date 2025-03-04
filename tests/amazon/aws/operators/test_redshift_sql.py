from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred

from astronomer.providers.amazon.aws.operators.redshift_sql import (
    RedshiftSQLOperatorAsync,
)
from astronomer.providers.amazon.aws.triggers.redshift_sql import RedshiftSQLTrigger
from tests.utils.airflow_util import create_context


class TestRedshiftSQLOperatorAsync:
    TASK_ID = "fetch_data"
    TASK = RedshiftSQLOperatorAsync(
        task_id=TASK_ID,
        sql="select * from any",
        params={},
    )

    @mock.patch("astronomer.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.execute_query")
    def test_redshiftsql_op_async(self, mock_execute):
        mock_execute.return_value = [], {}

        with pytest.raises(TaskDeferred) as exc:
            self.TASK.execute(create_context(self.TASK))
        assert isinstance(exc.value.trigger, RedshiftSQLTrigger), "Trigger is not a RedshiftSQLTrigger"

    @mock.patch("astronomer.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.execute_query")
    def test_redshiftsql_op_async_execute_query_error(self, mock_execute, context):
        mock_execute.return_value = [], {"status": "error", "message": "Test exception"}

        with pytest.raises(AirflowException):
            self.TASK.execute(context)

    def test_redshiftsql_op_async_execute_failure(self, context):
        """Tests that an AirflowException is raised in case of error event"""

        with pytest.raises(AirflowException):
            self.TASK.execute_complete(
                context=None, event={"status": "error", "message": "test failure message"}
            )

    @pytest.mark.parametrize(
        "event",
        [None, {"status": "success", "message": "Job completed"}],
    )
    def test_redshiftsql_op_async_execute_complete(self, event):
        """Asserts that logging occurs as expected"""

        with mock.patch.object(self.TASK.log, "info") as mock_log_info:
            self.TASK.execute_complete(context=None, event=event)
        mock_log_info.assert_called_with("%s completed successfully.", self.TASK_ID)
