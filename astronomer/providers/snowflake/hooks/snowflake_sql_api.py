import uuid
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import aiohttp
import requests
from airflow import AirflowException
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

from astronomer.providers.snowflake.hooks.sql_api_generate_jwt import JWTGenerator


class SnowflakeSqlApiHookAsync(SnowflakeHook):
    """
    A client to interact with Snowflake using SQL API  and allows submitting
    multiple SQL statements in a single request. In combination with aiohttp, make post request to submit SQL
    statements for execution, poll to check the status of the execution of a statement. Fetch query results
    asynchronously.

    This hook requires the snowflake_conn_id connection. This hooks mainly uses account, schema, database, warehouse,
    private_key_file or private_key_content field must be setup in the connection.
    Other inputs can be defined in the connection or hook instantiation.

    :param snowflake_conn_id: Reference to
        :ref:`Snowflake connection id<howto/connection:snowflake>`
    :param account: snowflake account name
    :param authenticator: authenticator for Snowflake.
        'snowflake' (default) to use the internal Snowflake authenticator
        'externalbrowser' to authenticate using your web browser and
        Okta, ADFS or any other SAML 2.0-compliant identify provider
        (IdP) that has been defined for your account
        'https://<your_okta_account_name>.okta.com' to authenticate
        through native Okta.
    :param warehouse: name of snowflake warehouse
    :param database: name of snowflake database
    :param region: name of snowflake region
    :param role: name of snowflake role
    :param schema: name of snowflake schema
    :param session_parameters: You can set session-level parameters at
        the time you connect to Snowflake
    :param token_life_time: lifetime of the JWT Token in timedelta
    :param token_renewal_delta: Renewal time of the JWT Token in  timedelta
    """

    LIFETIME = timedelta(minutes=59)  # The tokens will have a 59 minute lifetime
    RENEWAL_DELTA = timedelta(minutes=54)  # Tokens will be renewed after 54 minutes

    def __init__(
        self,
        snowflake_conn_id: str,
        token_life_time: timedelta = LIFETIME,
        token_renewal_delta: timedelta = RENEWAL_DELTA,
        *args: Any,
        **kwargs: Any,
    ):
        self.snowflake_conn_id = snowflake_conn_id
        self.token_life_time = token_life_time
        self.token_renewal_delta = token_renewal_delta
        super().__init__(snowflake_conn_id, *args, **kwargs)
        self.private_key = None

    def get_private_key(self) -> None:
        """Gets the private key from snowflake connection"""
        conn = self.get_connection(self.snowflake_conn_id)

        # If private_key_file is specified in the extra json, load the contents of the file as a private key.
        # If private_key_content is specified in the extra json, use it as a private key.
        # As a next step, specify this private key in the connection configuration.
        # The connection password then becomes the passphrase for the private key.
        # If your private key is not encrypted (not recommended), then leave the password empty.

        private_key_file = conn.extra_dejson.get(
            "extra__snowflake__private_key_file"
        ) or conn.extra_dejson.get("private_key_file")
        private_key_content = conn.extra_dejson.get(
            "extra__snowflake__private_key_content"
        ) or conn.extra_dejson.get("private_key_content")

        private_key_pem = None
        if private_key_content and private_key_file:
            raise AirflowException(
                "The private_key_file and private_key_content extra fields are mutually exclusive. "
                "Please remove one."
            )
        elif private_key_file:
            private_key_pem = Path(private_key_file).read_bytes()
        elif private_key_content:
            private_key_pem = private_key_content.encode()

        if private_key_pem:
            passphrase = None
            if conn.password:
                passphrase = conn.password.strip().encode()

            self.private_key = serialization.load_pem_private_key(
                private_key_pem, password=passphrase, backend=default_backend()  # type: ignore[assignment]
            )

    def execute_query(
        self, sql: str, statement_count: int, query_tag: str = "", bindings: Optional[Dict[str, Any]] = None
    ) -> List[str]:
        """
        Using SnowflakeSQL API, run the query in snowflake by making API request

        :param sql: the sql string to be executed with possibly multiple statements
        :param statement_count: set the MULTI_STATEMENT_COUNT field to the number of SQL statements in the request
        :param query_tag: (Optional) Query tag that you want to associate with the SQL statement.
            For details, see https://docs.snowflake.com/en/sql-reference/parameters.html#label-query-tag parameter.
        :param bindings: (Optional) Values of bind variables in the SQL statement.
            When executing the statement, Snowflake replaces placeholders (? and :name) in
            the statement with these specified values.
        """
        conn_config = self._get_conn_params()

        req_id = uuid.uuid4()
        url = "https://{}.snowflakecomputing.com/api/v2/statements".format(conn_config["account"])
        params: Optional[Union[Dict[str, Any]]] = {"requestId": str(req_id), "async": True, "pageSize": 10}
        headers = self.get_headers()
        if bindings is None:
            bindings = {}
        data = {
            "statement": sql,
            "resultSetMetaData": {"format": "json"},
            "database": conn_config["database"],
            "schema": conn_config["schema"],
            "warehouse": conn_config["warehouse"],
            "role": conn_config["role"],
            "bindings": bindings,
            "parameters": {
                "MULTI_STATEMENT_COUNT": statement_count,
                "query_tag": query_tag,
            },
        }
        response = requests.post(url, json=data, headers=headers, params=params)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:  # pragma: no cover
            raise AirflowException(
                f"Response: {e.response.content}, " f"Status Code: {e.response.status_code}"
            )  # pragma: no cover
        json_response = response.json()
        self.log.info("Snowflake SQL POST API response: %s", json_response)
        if "statementHandles" in json_response:
            self.query_ids = json_response["statementHandles"]
        elif "statementHandle" in json_response:
            self.query_ids.append(json_response["statementHandle"])
        else:
            raise AirflowException("No statementHandle/statementHandles present in response")
        return self.query_ids

    def get_headers(self) -> Dict[str, Any]:
        """Based on the private key, and with connection details JWT Token is generated and header is formed"""
        if not self.private_key:
            self.get_private_key()
        conn_config = self._get_conn_params()

        # Get the JWT token from the connection details and the private key
        token = JWTGenerator(
            conn_config["account"],  # type: ignore[arg-type]
            conn_config["user"],  # type: ignore[arg-type]
            private_key=self.private_key,
            lifetime=self.token_life_time,
            renewal_delay=self.token_renewal_delta,
        ).get_token()

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "User-Agent": "snowflakeSQLAPI/1.0",
            "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT",
        }
        return headers

    def get_request_url_header_params(self, query_id: str) -> Tuple[Dict[str, Any], Dict[str, Any], str]:
        """
        Build the request header Url with account name identifier and query id from the connection params

        :param query_id: statement handles query ids for the individual statements.
        """
        conn_config = self._get_conn_params()
        req_id = uuid.uuid4()
        header = self.get_headers()
        params = {"requestId": str(req_id), "page": 2, "pageSize": 10}
        url = "https://{}.snowflakecomputing.com/api/v2/statements/{}".format(
            conn_config["account"], query_id
        )
        return header, params, url

    def check_query_output(self, query_ids: List[str]) -> None:
        """
        Based on the query ids passed as the parameter make HTTP request to snowflake SQL API and logs the response

        :param query_ids: statement handles query id for the individual statements.
        """
        for query_id in query_ids:
            header, params, url = self.get_request_url_header_params(query_id)
            try:
                response = requests.get(url, headers=header, params=params)
                response.raise_for_status()
                self.log.info(response.json())
            except requests.exceptions.HTTPError as e:
                raise AirflowException(
                    f"Response: {e.response.content}, Status Code: {e.response.status_code}"
                )

    async def get_sql_api_query_status(self, query_id: str) -> Dict[str, Union[str, List[str]]]:
        """
        Based on the query id async HTTP request is made to snowflake SQL API and return response.

        :param query_id: statement handle id for the individual statements.
        """
        self.log.info("Retrieving status for query id %s", {query_id})
        header, params, url = self.get_request_url_header_params(query_id)
        async with aiohttp.ClientSession(headers=header) as session:
            async with session.get(url, params=params) as response:
                status_code = response.status
                resp = await response.json()
                self.log.info("Snowflake SQL GET statements status API response: %s", resp)
                if status_code == 202:
                    return {"status": "running", "message": "Query statements are still running"}
                elif status_code == 422:
                    return {"status": "error", "message": resp["message"]}
                elif status_code == 200:
                    statement_handles = []
                    if "statementHandles" in resp and resp["statementHandles"]:
                        statement_handles = resp["statementHandles"]
                    elif "statementHandle" in resp and resp["statementHandle"]:
                        statement_handles.append(resp["statementHandle"])
                    return {
                        "status": "success",
                        "message": resp["message"],
                        "statement_handles": statement_handles,
                    }
                else:
                    return {"status": "error", "message": resp["message"]}
