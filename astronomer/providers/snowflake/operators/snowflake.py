from __future__ import annotations
import inspect
import sys
from textwrap import dedent

import typing
from datetime import timedelta
from typing import Any, Callable, List, Collection, Iterable, Mapping

from airflow.exceptions import AirflowException
from airflow.utils.context import Context, context_copy_partial
from airflow.operators.python import _BasePythonVirtualenvOperator, get_current_context

try:
    from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
except ImportError:  # pragma: no cover
    # For apache-airflow-providers-snowflake > 3.3.0
    # currently added type: ignore[no-redef, attr-defined] and pragma: no cover because this import
    # path won't be available in current setup
    from airflow.providers.common.sql.operators.sql import (  # type: ignore[assignment]
        SQLExecuteQueryOperator as SnowflakeOperator,
    )

from astronomer.providers.snowflake.hooks.snowflake import (
    SnowflakeHookAsync,
    fetch_all_snowflake_handler,
    SnowServicesHook,
)
from astronomer.providers.snowflake.hooks.snowflake_sql_api import (
    SnowflakeSqlApiHookAsync,
)
from astronomer.providers.snowflake.triggers.snowflake_trigger import (
    SnowflakeSqlApiTrigger,
    SnowflakeTrigger,
    get_db_hook,
)
from astronomer.providers.utils.typing_compat import Context


class SnowflakeOperatorAsync(SnowflakeOperator):
    """
    - SnowflakeOperatorAsync uses the snowflake python connector ``execute_async`` method to submit a database command
      for asynchronous execution.
    - Submit multiple queries in parallel without waiting for each query to complete.
    - Accepts list of queries or multiple queries with ‘;’ semicolon separated string and params. It loops through the
      queries and execute them in sequence. Uses execute_async method to run the query
    - Once a query is submitted, it executes the query from one connection and gets the query IDs from the
      response and passes it to the Triggerer and closes the connection (so that the worker slots can be freed up).
    - The trigger gets the list of query IDs as input and polls every few seconds to snowflake and checks
      for the query status based on the query ID from different connection.

    Where can this operator fit in?
         - Execute time taking queries which can be executed in parallel
         - For batch based operation like copy or inserting the data in parallel.

    Best practices:
         - Ensure that you know which queries are dependent upon other queries before you run any queries in parallel.
           Some queries are interdependent and order sensitive, and therefore not suitable for parallelizing.
           For example, obviously an INSERT statement should not start until after the corresponding to CREATE TABLE
           statement has finished.
         - Ensure that you do not run too many queries for the memory that you have available.
           Running multiple queries in parallel typically consumes more memory,
           especially if more than one set of results is stored in memory at the same time.
         - Ensure that transaction control statements (BEGIN, COMMIT, and ROLLBACK) do not execute in parallel
           with other statements.

    .. seealso::
        - `Snowflake Async Python connector <https://docs.snowflake.com/en/user-guide/python-connector-example.html#label-python-connector-querying-data-asynchronous.>`_
        - `Best Practices <https://docs.snowflake.com/en/user-guide/python-connector-example.html#best-practices-for-asynchronous-queries>`_

    :param snowflake_conn_id: Reference to Snowflake connection id
    :param sql: the sql code to be executed. (templated)
    :param autocommit: if True, each command is automatically committed.
        (default value: True)
    :param parameters: (optional) the parameters to render the SQL query with.
    :param warehouse: name of warehouse (will overwrite any warehouse
        defined in the connection's extra JSON)
    :param database: name of database (will overwrite database defined
        in connection)
    :param schema: name of schema (will overwrite schema defined in
        connection)
    :param role: name of role (will overwrite any role defined in
        connection's extra JSON)
    :param authenticator: authenticator for Snowflake.
        'snowflake' (default) to use the internal Snowflake authenticator
        'externalbrowser' to authenticate using your web browser and
        Okta, ADFS or any other SAML 2.0-compliant identify provider
        (IdP) that has been defined for your account
        'https://<your_okta_account_name>.okta.com' to authenticate
        through native Okta.
    :param session_parameters: You can set session-level parameters at
        the time you connect to Snowflake
    :param handler: (optional) the function that will be applied to the cursor (default: fetch_all_handler).
    :param return_last: (optional) if return the result of only last statement (default: True).
    :param poll_interval: the interval in seconds to poll the query
    """  # noqa

    def __init__(
        self,
        *,
        snowflake_conn_id: str = "snowflake_default",
        warehouse: str | None = None,
        database: str | None = None,
        role: str | None = None,
        schema: str | None = None,
        authenticator: str | None = None,
        session_parameters: dict[str, Any] | None = None,
        poll_interval: int = 5,
        handler: Callable[[Any], Any] = fetch_all_snowflake_handler,
        return_last: bool = True,
        **kwargs: Any,
    ) -> None:
        self.poll_interval = poll_interval
        self.warehouse = warehouse
        self.database = database
        self.role = role
        self.schema = schema
        self.authenticator = authenticator
        self.session_parameters = session_parameters
        self.snowflake_conn_id = snowflake_conn_id
        if self.__class__.__base__.__name__ != "SnowflakeOperator":
            # It's better to do str check of the parent class name because currently SnowflakeOperator
            # is deprecated and in future OSS SnowflakeOperator may be removed
            if any(
                [warehouse, database, role, schema, authenticator, session_parameters]
            ):  # pragma: no cover
                hook_params = kwargs.pop("hook_params", {})  # pragma: no cover
                kwargs["hook_params"] = {
                    "warehouse": warehouse,
                    "database": database,
                    "role": role,
                    "schema": schema,
                    "authenticator": authenticator,
                    "session_parameters": session_parameters,
                    **hook_params,
                }  # pragma: no cover
            super().__init__(conn_id=snowflake_conn_id, **kwargs)  # pragma: no cover
        else:
            super().__init__(**kwargs)
        self.handler = handler
        self.return_last = return_last

    def get_db_hook(self) -> SnowflakeHookAsync:
        """Get the Snowflake Hook"""
        return get_db_hook(self.snowflake_conn_id)

    def execute(self, context: Context) -> None:
        """
        Make a sync connection to snowflake and run query in execute_async
        function in snowflake and close the connection and with the query ids, fetch the status of the query.
        By deferring the SnowflakeTrigger class pass along with query ids.
        """
        self.log.info("Executing: %s", self.sql)

        default_query_tag = f"airflow_openlineage_{self.task_id}_{self.dag.dag_id}_{context['ti'].try_number}"
        query_tag = (
            self.parameters.get("query_tag", default_query_tag)
            if isinstance(self.parameters, dict)
            else default_query_tag
        )
        session_query_tag = f"ALTER SESSION SET query_tag = '{query_tag}';"
        if isinstance(self.sql, str):
            self.sql = "\n".join([session_query_tag, self.sql])
        else:
            session_query_list = [session_query_tag]
            session_query_list.extend(self.sql)
            self.sql = session_query_list

        self.log.info("SQL after adding query tag: %s", self.sql)

        hook = self.get_db_hook()
        hook.run(self.sql, parameters=self.parameters)  # type: ignore[arg-type]
        self.query_ids = hook.query_ids

        if self.do_xcom_push:
            context["ti"].xcom_push(key="query_ids", value=self.query_ids)

        self.defer(
            timeout=self.execution_timeout,
            trigger=SnowflakeTrigger(
                task_id=self.task_id,
                poll_interval=self.poll_interval,
                query_ids=self.query_ids,
                snowflake_conn_id=self.snowflake_conn_id,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict[str, str | list[str]] | None = None) -> Any:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        self.log.info("SQL in execute_complete: %s", self.sql)
        if event:
            if "status" in event and event["status"] == "error":
                msg = "{}: {}".format(event["type"], event["message"])
                raise AirflowException(msg)
            elif "status" in event and event["status"] == "success":
                hook = self.get_db_hook()
                qids = typing.cast(List[str], event["query_ids"])
                results = hook.check_query_output(qids, self.handler, self.return_last)
                self.log.info("%s completed successfully.", self.task_id)
                if self.do_xcom_push:
                    return results
        else:
            raise AirflowException("Did not receive valid event from the trigerrer")


class SnowflakeSqlApiOperatorAsync(SnowflakeOperator):
    """
    Implemented Async Snowflake SQL API Operator to support multiple SQL statements sequentially,
    which is the behavior of the SnowflakeOperator, the Snowflake SQL API allows submitting
    multiple SQL statements in a single request. In combination with aiohttp, make post request to submit SQL
    statements for execution, poll to check the status of the execution of a statement. Fetch query results
    concurrently.
    This Operator currently uses key pair authentication, so you need tp provide private key raw content or
    private key file path in the snowflake connection along with other details

    .. seealso::

        `Snowflake SQL API key pair Authentication <https://docs.snowflake.com/en/developer-guide/sql-api/authenticating.html#label-sql-api-authenticating-key-pair>`_

    Where can this operator fit in?
         - To execute multiple SQL statements in a single request
         - To execute the SQL statement asynchronously and to execute standard queries and most DDL and DML statements
         - To develop custom applications and integrations that perform queries
         - To create provision users and roles, create table, etc.

    The following commands are not supported:
        - The PUT command (in Snowflake SQL)
        - The GET command (in Snowflake SQL)
        - The CALL command with stored procedures that return a table(stored procedures with the RETURNS TABLE clause).

    .. seealso::

        - `Snowflake SQL API <https://docs.snowflake.com/en/developer-guide/sql-api/intro.html#introduction-to-the-sql-api>`_
        - `API Reference <https://docs.snowflake.com/en/developer-guide/sql-api/reference.html#snowflake-sql-api-reference>`_
        - `Limitation on snowflake SQL API <https://docs.snowflake.com/en/developer-guide/sql-api/intro.html#limitations-of-the-sql-api>`_

    :param snowflake_conn_id: Reference to Snowflake connection id
    :param sql: the sql code to be executed. (templated)
    :param autocommit: if True, each command is automatically committed.
        (default value: True)
    :param parameters: (optional) the parameters to render the SQL query with.
    :param warehouse: name of warehouse (will overwrite any warehouse
        defined in the connection's extra JSON)
    :param database: name of database (will overwrite database defined
        in connection)
    :param schema: name of schema (will overwrite schema defined in
        connection)
    :param role: name of role (will overwrite any role defined in
        connection's extra JSON)
    :param authenticator: authenticator for Snowflake.
        'snowflake' (default) to use the internal Snowflake authenticator
        'externalbrowser' to authenticate using your web browser and
        Okta, ADFS or any other SAML 2.0-compliant identify provider
        (IdP) that has been defined for your account
        'https://<your_okta_account_name>.okta.com' to authenticate
        through native Okta.
    :param session_parameters: You can set session-level parameters at
        the time you connect to Snowflake
    :param poll_interval: the interval in seconds to poll the query
    :param statement_count: Number of SQL statement to be executed
    :param token_life_time: lifetime of the JWT Token
    :param token_renewal_delta: Renewal time of the JWT Token
    :param bindings: (Optional) Values of bind variables in the SQL statement.
            When executing the statement, Snowflake replaces placeholders (? and :name) in
            the statement with these specified values.
    """  # noqa

    LIFETIME = timedelta(minutes=59)  # The tokens will have a 59 minutes lifetime
    RENEWAL_DELTA = timedelta(minutes=54)  # Tokens will be renewed after 54 minutes

    def __init__(
        self,
        *,
        snowflake_conn_id: str = "snowflake_default",
        warehouse: str | None = None,
        database: str | None = None,
        role: str | None = None,
        schema: str | None = None,
        authenticator: str | None = None,
        session_parameters: dict[str, Any] | None = None,
        poll_interval: int = 5,
        statement_count: int = 0,
        token_life_time: timedelta = LIFETIME,
        token_renewal_delta: timedelta = RENEWAL_DELTA,
        bindings: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        self.warehouse = warehouse
        self.database = database
        self.role = role
        self.schema = schema
        self.authenticator = authenticator
        self.session_parameters = session_parameters
        self.snowflake_conn_id = snowflake_conn_id
        self.poll_interval = poll_interval
        self.statement_count = statement_count
        self.token_life_time = token_life_time
        self.token_renewal_delta = token_renewal_delta
        self.bindings = bindings
        self.execute_async = False
        if self.__class__.__base__.__name__ != "SnowflakeOperator":
            # It's better to do str check of the parent class name because currently SnowflakeOperator
            # is deprecated and in future OSS SnowflakeOperator may be removed
            if any(
                [warehouse, database, role, schema, authenticator, session_parameters]
            ):  # pragma: no cover
                hook_params = kwargs.pop("hook_params", {})  # pragma: no cover
                kwargs["hook_params"] = {
                    "warehouse": warehouse,
                    "database": database,
                    "role": role,
                    "schema": schema,
                    "authenticator": authenticator,
                    "session_parameters": session_parameters,
                    **hook_params,
                }
            super().__init__(conn_id=snowflake_conn_id, **kwargs)  # pragma: no cover
        else:
            super().__init__(**kwargs)

    def execute(self, context: Context) -> None:
        """
        Make a POST API request to snowflake by using SnowflakeSQL and execute the query to get the ids.
        By deferring the SnowflakeSqlApiTrigger class passed along with query ids.
        """
        self.log.info("Executing: %s", self.sql)
        hook = SnowflakeSqlApiHookAsync(
            snowflake_conn_id=self.snowflake_conn_id,
            token_life_time=self.token_life_time,
            token_renewal_delta=self.token_renewal_delta,
        )
        hook.execute_query(
            self.sql, statement_count=self.statement_count, bindings=self.bindings  # type: ignore[arg-type]
        )
        self.query_ids = hook.query_ids
        self.log.info("List of query ids %s", self.query_ids)

        if self.do_xcom_push:
            context["ti"].xcom_push(key="query_ids", value=self.query_ids)

        self.defer(
            timeout=self.execution_timeout,
            trigger=SnowflakeSqlApiTrigger(
                poll_interval=self.poll_interval,
                query_ids=self.query_ids,
                snowflake_conn_id=self.snowflake_conn_id,
                token_life_time=self.token_life_time,
                token_renewal_delta=self.token_renewal_delta,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict[str, str | list[str]] | None = None) -> None:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event:
            if "status" in event and event["status"] == "error":
                msg = "{}: {}".format(event["status"], event["message"])
                raise AirflowException(msg)
            elif "status" in event and event["status"] == "success":
                hook = SnowflakeSqlApiHookAsync(snowflake_conn_id=self.snowflake_conn_id)
                query_ids = typing.cast(List[str], event["statement_query_ids"])
                hook.check_query_output(query_ids)
                self.log.info("%s completed successfully.", self.task_id)
        else:
            self.log.info("%s completed successfully.", self.task_id)

class SnowServicesPythonOperator(_BasePythonVirtualenvOperator):
    """
    Runs a function in a Snowservices service runner environment.  

    The function must be defined using def, and not be
    part of a class. All imports must happen inside the function
    and no variables outside the scope may be referenced. A global scope
    variable named virtualenv_string_args will be available (populated by
    string_args). In addition, one can pass stuff through op_args and op_kwargs, and one
    can use a return value.
    
    ##TODO: update docs
    Task XCOMs will be saved in a Snowflake stage and return values to Airflow (plain xcom) will list 
    this stage location and filename (ie. @mystage/path/file).

    :param snowflake_conn_id: connection to use when running code within the Snowservices runner.
    :type snowflake_conn_id: str  (default is snowflake_default)
    :param runner_endpoint: URL endpoint of the instantiated snowservice runner (ie. ws://<hostnam>:<port>/<endpoint>)
    :type runner_endpoint: str
    :param python_callable: Function to decorate
    :type python_callable: Callable 
    :param python: Python version (ie. '<maj>.<min>').  Callable will run in a PythonVirtualenvOperator on the runner.  
        If not set will use default python version on runner.
    :type python: str:
# :param python_version: Python version to build if using 'virtualenv'. If set to None the runner will try to build 
# a virtualenv based on the python version of the Airflow scheduler.  If 'python' param is not 'virtualenv' python_version is ignored.
    :param requirements: Optional list of python dependencies or a path to a requirements.txt file to be installed for the callable.
    :type requirements: list | str
    :param op_kwargs: a dictionary of keyword arguments that will get unpacked in your function (templated)
    :param op_args: a list of positional arguments that will get unpacked when calling your callable (templated)
    :param string_args: Strings that are present in the global var virtualenv_string_args,
        available to python_callable at runtime as a list[str]. Note that args are split
        by newline.
    :param templates_dict: a dictionary where the values are templates that
        will get templated by the Airflow engine sometime between
        ``__init__`` and ``execute`` takes place and are made available
        in your callable's context after the template has been applied
    :param templates_exts: a list of file extensions to resolve while
        processing templated fields, for examples ``['.sql', '.hql']``
    :param expect_airflow: expect Airflow to be installed in the target environment. If true, the operator
        will raise warning if Airflow is not installed, and it will attempt to load Airflow
        macros when starting.
    """

    #template_fields: Sequence[str] = tuple({"python"} | set(PythonOperator.template_fields))

    def __init__(
        self,
        *,
        runner_endpoint: str, 
        python_callable: Callable,
        snowflake_conn_id: str = 'snowflake_default',
        python: str | None = None,
        # python_version: str | None = None,
        requirements: None | Iterable[str] | str = None,
        use_dill: bool = False,
        system_site_packages: bool = True,
        pip_install_options: list[str] | None = None,
        op_args: Collection[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        string_args: Iterable[str] | None = None,
        templates_dict: dict | None = None,
        templates_exts: list[str] | None = None,
        expect_airflow: bool = True,
        expect_pendulum: bool = False,
        **kwargs,
    ):

        if not requirements:
            self.requirements: list[str] | str = []
        elif isinstance(requirements, str):
            try: 
                with open(requirements, 'r') as requirements_file:
                    self.requirements = requirements_file.read().splitlines()
            except:
                raise FileNotFoundError(f'Specified requirements file {requirements} does not exist or is not readable.')
        else:
            self.requirements = list(requirements)
        
        self.system_site_packages = system_site_packages
        self.pip_install_options = pip_install_options
        self.snowflake_conn_id = snowflake_conn_id
        #self.venv_python_version = python_version
        #self.source_python_version = f"{sys.version_info.major}.{sys.version_info.minor}"
        self.use_dill = use_dill
        self.hook = SnowServicesHook(snowflake_conn_id=self.snowflake_conn_id)
        self.snowflake_connection_uri = self.hook.get_uri()  #TODO: get correct uri
        self.target_python = python
        self.expect_airflow = expect_airflow
        self.expect_pendulum = expect_pendulum
        self.string_args = string_args
        self.templates_dict = templates_dict
        self.templates_exts = templates_exts
        self.runner_endpoint = runner_endpoint
        self.system_site_packages = True
        
        super().__init__(
            python_callable=python_callable,
            use_dill = use_dill,
            op_args=op_args,
            op_kwargs=op_kwargs,
            string_args=string_args,
            templates_dict=templates_dict,
            templates_exts=templates_exts,
            expect_airflow = expect_airflow,
            **kwargs,
        )

    def _build_payload(self, context):

        payload = dict(
            python_callable_str = dedent(inspect.getsource(self.python_callable)),
            python_callable_name = self.python_callable.__name__,
            requirements = self.requirements,
            pip_install_options = self.pip_install_options,
            # venv_python_version = self.venv_python_version,
            # source_python_version = self.source_python_version, 
            snowflake_connection_uri = self.snowflake_connection_uri,
            use_dill = self.use_dill,
            system_site_packages = self.system_site_packages,
            target_python = self.target_python,
            dag_id = self.dag_id,
            task_id = self.task_id,
            task_start_date = context["ts"], #"2022-01-01T00:00:00Z00:00"
            op_args = self.op_args,
            op_kwargs = self.op_kwargs,
            templates_dict = self.templates_dict,
            templates_exts = self.templates_exts,
            expect_airflow = self.expect_airflow,
            expect_pendulum = self.expect_pendulum,
            string_args = self.string_args,
            xcom_input = context["ti"].xcom_pull(),
        )
        
        return payload

    def _iter_serializable_context_keys(self):
        yield from self.BASE_SERIALIZABLE_CONTEXT_KEYS
        if self.system_site_packages or "apache-airflow" in self.requirements:
            yield from self.AIRFLOW_SERIALIZABLE_CONTEXT_KEYS
            yield from self.PENDULUM_SERIALIZABLE_CONTEXT_KEYS
        elif "pendulum" in self.requirements:
            yield from self.PENDULUM_SERIALIZABLE_CONTEXT_KEYS

    def execute(self, context: Context) -> Any:
        serializable_keys = set(self._iter_serializable_context_keys())
        serializable_context = context_copy_partial(context, serializable_keys)


        self.payload = self._build_payload(context)

        # import json
        # with open(f'/tmp/{self.task_id}_payload.json', 'w') as f:
        #     f.write(json.dumps(self.payload))

        # with open(f'/tmp/{self.task_id}_context.json', 'w') as f:
        #     f.write(json.dumps([context["ti"].xcom_pull(), context["ts"]]))



        return super().execute(context=serializable_context)

    def execute_callable(self):
        import asyncio

        responses =  asyncio.run(self._execute_python_callable_in_snowservices(self.payload))

        return responses
        
    async def _execute_python_callable_in_snowservices(self, payload):
        import aiohttp

        # payload['xcom_input'] = context["ti"].xcom_pull()

        responses = []
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(self.runner_endpoint) as websocket_runner:
                await websocket_runner.send_json(payload)

                while True:
                    response = await websocket_runner.receive_json()
                    #responses += [response]

                    # make sure we're not receiving empty responses
                    assert response != None

                    if response['type'] == 'results':
                        self.log.info(response)
                        return response['output']
                    elif response['type'] in ["log", "execution_log", "infra"]:
                        self.log.info(response)
                    elif response['type'] == "error":
                        self.log.error(response)
                        raise AirflowException("Error occurred in Task run.")
