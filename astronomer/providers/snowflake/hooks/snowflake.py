from __future__ import annotations

import asyncio
from contextlib import closing
from io import StringIO
from typing import Any, Callable
import json
import yaml
from uuid import uuid4
import os
import requests


from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from asgiref.sync import sync_to_async

from snowflake.connector import DictCursor, ProgrammingError
from snowflake.connector.constants import QueryStatus
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.connector.util_text import split_statements

from astronomer.providers.snowflake.utils.build_spec_file import create_k8s_spec


def fetch_all_snowflake_handler(
    cursor: SnowflakeCursor,
) -> list[tuple[Any, ...]] | list[dict[str, Any]] | None:
    """Handler for SnowflakeCursor to return results"""
    return cursor.fetchall()


def fetch_one_snowflake_handler(cursor: SnowflakeCursor) -> dict[str, Any] | tuple[Any, ...] | None:
    """Handler for SnowflakeCursor to return results"""
    return cursor.fetchone()


class SnowflakeHookAsync(SnowflakeHook):
    """
    A client to interact with Snowflake.

    This hook requires the snowflake_conn_id connection. The snowflake host, login,
    and, password field must be setup in the connection. Other inputs can be defined
    in the connection or hook instantiation. If used with the S3ToSnowflakeOperator
    add 'aws_access_key_id' and 'aws_secret_access_key' to extra field in the connection.

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
    """

    def run(  # type: ignore[override]
        self,
        sql: str | list[str],
        autocommit: bool = True,
        parameters: dict | None = None,  # type: ignore[type-arg]
    ) -> list[str]:
        """
        Runs a SQL command or a list of SQL commands.

        :param sql: the sql string to be executed with possibly multiple statements,
          or a list of sql statements to execute
        :param autocommit: What to set the connection's autocommit setting to before executing the query.
        :param parameters: The parameters to render the SQL query with.
        """
        self.query_ids = []
        with closing(self.get_conn()) as conn:
            self.set_autocommit(conn, autocommit)

            self.log.info("SQL statement to be executed: %s ", sql)
            if isinstance(sql, str):
                split_statements_tuple = split_statements(StringIO(sql))
                sql = [sql_string for sql_string, _ in split_statements_tuple if sql_string]

            self.log.debug("Executing %d statements against Snowflake DB", len(sql))
            with closing(conn.cursor(DictCursor)) as cur:

                for sql_statement in sql:

                    self.log.info("Running statement: %s, parameters: %s", sql_statement, parameters)
                    if parameters:
                        cur.execute_async(sql_statement, parameters)
                    else:
                        cur.execute_async(sql_statement)
                    query_id = cur.sfqid
                    self.log.info("Snowflake query id: %s", query_id)
                    self.query_ids.append(query_id)

            # If autocommit was set to False for db that supports autocommit,
            # or if db does not supports autocommit, we do a manual commit.
            if not self.get_autocommit(conn):
                conn.commit()
        return self.query_ids

    def check_query_output(
        self, query_ids: list[str], handler: Callable[[Any], Any] | None = None, return_last: bool = True
    ) -> Any | list[Any] | None:
        """Once the query is finished fetch the result and log it in airflow"""
        with closing(self.get_conn()) as conn:
            self.set_autocommit(conn, True)
            with closing(conn.cursor(DictCursor)) as cur:
                results = []
                for query_id in query_ids:
                    cur.get_results_from_sfqid(query_id)
                    if handler is not None:
                        result = handler(cur)
                        results.append(result)
                    self.log.info("Rows affected: %s", cur.rowcount)
                    self.log.info("Snowflake query id: %s", query_id)
            conn.commit()
        if handler is None:
            return None
        elif return_last:
            return results[-1]
        else:
            return results

    async def get_query_status(
        self, query_ids: list[str], poll_interval: float
    ) -> dict[str, str | list[str]]:
        """Get the Query status by query ids."""
        try:
            sfqid = []
            async_connection = await sync_to_async(self.get_conn)()
            try:
                with closing(async_connection) as conn:
                    for query_id in query_ids:
                        while conn.is_still_running(conn.get_query_status_throw_if_error(query_id)):
                            await asyncio.sleep(poll_interval)  # pragma: no cover
                        status = conn.get_query_status(query_id)
                        if status == QueryStatus.SUCCESS:
                            self.log.info("The query finished successfully")
                            sfqid.append(query_id)
                        elif status == QueryStatus.ABORTING:
                            return {
                                "status": "error",
                                "message": "The query is in the process of being aborted on the server side.",
                                "type": "ABORTING",
                                "query_id": query_id,
                            }
                        elif status == QueryStatus.FAILED_WITH_ERROR:
                            return {
                                "status": "error",
                                "message": "The query finished unsuccessfully.",
                                "type": "FAILED_WITH_ERROR",
                                "query_id": query_id,
                            }
                        else:
                            return {"status": "error", "message": f"Unknown status: {status}"}
                    return {"status": "success", "query_ids": sfqid}
            except ProgrammingError as err:
                error_message = "Programming Error: {}".format(err)
                return {"status": "error", "message": error_message, "type": "ERROR"}
        except Exception as e:
            self.log.exception("Unexpected error when retrieving query status:")
            return {"status": "error", "message": str(e), "type": "ERROR"}

class SnowServicesHook(SnowflakeHook):
    """
    SnowServices Hook to create and manage Snowservices instances as well as return a SnowService instance endpoint URL.

    :param conn_id: Snowflake connection id
    :type conn_id: str
    :param account: snowflake account name
    :type account: str
    :param authenticator: authenticator for Snowflake.
        'snowflake' (default) to use the internal Snowflake authenticator
        'externalbrowser' to authenticate using your web browser and
        Okta, ADFS or any other SAML 2.0-compliant identify provider
        (IdP) that has been defined for your account
        'https://<your_okta_account_name>.okta.com' to authenticate
        through native Okta.
    :type authenticator: str
    :param warehouse: name of snowflake warehouse
    :type warehouse: str
    :param database: name of snowflake database
    :type database: str
    :param region: name of snowflake region
    :type region: str
    :param role: name of snowflake role
    :type role: str
    :param schema: name of snowflake schema
    :type schema: str
    :param session_parameters: You can set session-level parameters at
        the time you connect to Snowflake
    :type session_parameters: str
    """

    conn_name_attr = "snowflake_conn_id"
    default_conn_name = "snowflake_default"
    conn_type = "snowflake"
    hook_name = "SnowServicesHook"
    instance_types = ['standard_1', 'standard_2', 'standard_3', 'standard_4', 'standard_5']
    gpu_types = ['NvidiaA10', 'NvidiaTeslaV100', 'NvidiaAmpereA100']

    def __init__(self, *args, **kwargs) -> None:
        self.snowflake_conn_id = kwargs.get("snowflake_conn_id")
        self.conn_id = kwargs.get("conn_id")
        self.local_test = kwargs.get("local_test") or None

        super().__init__(*args, **kwargs)

    def create_pool(self, 
        pool_name : str, 
        instance_family:str = 'standard_1' , 
        replace_existing = False, 
        min_nodes = 1, 
        max_nodes = 1, 
        gpu_name : str = None):
        """
        Create (or replace an existing) Snowservices compute pool.

        Todo: check instance/gpu for compatibility

        :param pool_name: Name of compute pool to create
        :type pool_name: str
        :param instance_family: Compute node instance family (ie. STANDARD_<1-5>)
        :type instance_family: str
        :param replace_existing: Whether an existing compute pool should be replaced or exit with failure.
        :type replace_existing: bool
        :param min_nodes: The minimum number of nodes for scaling group
        :type min_nodes: int
        :param max_nodes: The maximum number of nodes to scale to
        :type max_nodes: int
        :param gpu_name: Whether to use GPU nodes (currently limited to NVIDIAA10G)
        :type gpu_name: str
        """
        if self.local_test == 'astro_cli' or self.local_test == 'docker_desktop_k8s':
            raise NotImplementedError('No compute pool in local testing mode.')
            return None
        elif not self.local_test:
            if gpu_name and gpu_name.lower() not in self.gpu_types:
                raise AttributeError(f"Unsupported option {gpu_name} specified for gpu_name.")

            if instance_family and instance_family.lower() not in self.instance_types:
                raise AttributeError(f"Unsupported option {instance_family} specified for instance_family.")

            gpu_option_str = ''
            if gpu_name:
                if instance_family not in ['standard_1', 'standard_2', 'standard_3']:
                    raise AttributeError("Invalid combination of instance_family and gpu_name.")
                else:
                    gpu_option_str = f" GPU_OPTIONS = ( accelerator = {gpu_name} ) "

            replace_existing_str = ' IF NOT EXISTS ' if not replace_existing else ''

            print(f"CREATE COMPUTE POOL {replace_existing_str} {pool_name} \
                    MIN_NODES = {min_nodes} \
                    MAX_NODES = {max_nodes} \
                    INSTANCE_FAMILY = {instance_family} \
                    {gpu_option_str};")

            return pool_name
        else:
            raise AttributeError('Invalid option for local_test')
    
    def remove_pool(self, pool_name:str, force_all=False):
        """
        Remove an existing Snowservices compute pool.
        :param pool_name: Name of compute pool to drop (required)
        :type pool_name: str
        :param force_all: Forcibly delete all existing snowservices before dropping the pool
        :type force_all: bool
        """
        if self.local_test == 'astro_cli' or self.local_test == 'docker_desktop_k8s':
            raise NotImplementedError('No compute pool in local testing mode.')
        elif not self.local_test:    
            force_all = 'true' if force_all else 'false'
            print(f"ALTER SESSION SET COMPUTE_POOL_FORCE_DELETE_ALL_SNOWSERVICES_ON_DROP = {force_all};\
                    DROP COMPUTE POOL {pool_name};")
            return None
        else:
            raise AttributeError('Invalid option for local_test')
    
    def list_pools(self, name_prefix:str = None, regex_pattern:str = None, limit:int = None):
        """
        List current Snowservices compute pools

        :param name_prefix: List only pools with names starting with prefix.
        :type name_prefix: str
        :param regex_pattern: Provide a regex string to specify pool names.
        :type regex_pattern: str
        :param limit: Limit returned result to specific number.
        :type limit: int
        """
        ##TODO: Add starts FROM logic

        if self.local_test == 'astro_cli' or self.local_test == 'docker_desktop_k8s':
            raise NotImplementedError('No compute pool in local testing mode.')

        elif not self.local_test:    
            if name_prefix:
                prefix_str = f" STARTS WITH {name_prefix} "

            if regex_pattern: 
                like_str = f" LIKE {regex_pattern} "

            if limit:
                limit_str = f" LIMIT {limit} "

            response = 'List of compute pools.' #self.get_conn().cursor().execute(f"SHOW COMPUTE POOLS {like_str} {prefix_str} {limit_str};").fetchall()
            return response
        else:
            raise AttributeError('Invalid option for local_test')

    def create_service(self, 
        service_name : str, 
        pool_name: str, 
        runner_endpoint: str | None = None,
        runner_port: int | None = None,
        spec_file_name : str = None,
        replace_existing = 'False', 
        min_inst = 1, 
        max_inst = 1) -> str:
        """
        Create (or replace an existing) Snowservice using a build-runner.

        :param service_name: Name of Snowservice to create
        :type service_name: str
        :param pool_name: Compute pool to use for service execution
        :type pool_name: str
        :param runner_endpoint: Endpoint name for the snowservice runner.
        :type runner_endpoint: str
        :param runner_port: Port number (int) for the snowservice runner.
        :type runner_port: int
        :param replace_existing: Whether an existing service should be replaced or exit with failure.
        :type replace_existing: bool
        :param min_inst: The minimum number of nodes for scaling group
        :type min_inst: int
        :param max_inst: The maximum number of nodes to scale to
        :type max_inst: int
        :param spec_file_name: Optional path to an existing YAML specification for the service
        :type spec_file: str
        """
        if spec_file_name:
            if runner_endpoint or runner_port:
                runner_endpoint = runner_port = None
                raise Warning('Both spec_file_name and runner_endpoint / runner_port parameters provided. endpoint/port will be ignored.')
            if not os.access(spec_file_name, os.F_OK | os.R_OK):
                raise FileExistsError(f"Spec file {spec_file_name} does not exist or is not readable.")
            else:
                with open(spec_file_name, 'r') as f:
                    try:
                        k8s_spec = json.dumps(yaml.safe_load(f))
                    except yaml.YAMLError as exception:
                        raise exception

        if self.local_test == 'astro_cli':
            raise NotImplementedError('Create service via astro cli.')

        elif self.local_test == 'docker_desktop_k8s':
            from kubernetes import client, config, utils

            if not spec_file_name:                    
                k8s_spec = create_k8s_spec(service_name=service_name, 
                                           runner_endpoint=runner_endpoint,
                                           runner_port=runner_port,
                                           local_test=self.local_test,
                                           )
            
            config.load_kube_config()

            #create a namespace for this service and deploy services
            try:
                metadata_obj = client.V1ObjectMeta(name=service_name, namespace=service_name)
                namespace_obj = client.V1Namespace(metadata=metadata_obj)
                v1 = client.CoreV1Api()
                v1.create_namespace(namespace_obj)

                k8s_client = client.ApiClient()
                try:
                    for doc in k8s_spec:
                        utils.create_from_dict(k8s_client, doc, namespace=service_name)
                except:
                    v1.delete_namespace(name=service_name)
                    raise utils.FailToCreateError()
                    
            
            except Exception as e:
                print(e)
                if e.reason and e.status:
                    if e.reason == 'Conflict' and e.status == 409:
                        raise utils.FailToCreateError('Service already exists.') 
            
            return service_name

        elif not self.local_test:  
            replace_existing_str = ' IF NOT EXISTS ' if not replace_existing else ''
            
            temp_stage_postfix = str(uuid4()).replace('-','_')
            temp_stage_name = f'{service_name}_{temp_stage_postfix}'

            if not spec_file_name:
                k8s_spec = create_k8s_spec(service_name=service_name, 
                                           runner_endpoint=runner_endpoint,
                                           runner_port=runner_port,
                                           )

            spec_file_name = f'{temp_stage_name}_spec.yml'                    

            with open(spec_file_name, 'w') as f:
                yaml.dump(k8s_spec, f, default_flow_style=False)

            try:
                self.run( f"CREATE TEMPORARY STAGE {temp_stage_name}; \
                            PUT file://{spec_file_name} @{temp_stage_name} \
                                AUTO_COMPRESS = False \
                                SOURCE_COMPRESSION = NONE;") \
                            # CREATE SERVICE {replace_existing_str} {service_name} \
                            #     MIN_INSTANCES = {min_inst} \
                            #     MAX_INSTANCES = {max_inst} \
                            #     COMPUTE_POOL = {pool_name} \
                            #     SPEC = @{temp_stage_name}/{spec_file_name};")
            except Exception as e:
                print(e)
            finally:
                os.remove(spec_file_name)
    
            ##TODO: need wait loop or asycn operation to make sure it is up
            return service_name
        else:
            raise AttributeError('Invalid option for local_test')

    def suspend_service(self, service_name:str):
        if self.local_test == 'astro_cli' or self.local_test == 'docker_desktop_k8s':
            raise NotImplementedError('No suspend option in local testing mode.')
        elif not self.local_test:    
            print(f'ALTER SERVICE IF EXISTS {service_name} SUSPEND')
        else:
            raise AttributeError('Invalid option for local_test')
        return None

    def resume_service(self, service_name:str):
        if self.local_test == 'astro_cli' or self.local_test == 'docker_desktop_k8s':
            raise NotImplementedError('No resume option in local testing mode.')
        elif not self.local_test:    
            print(f'ALTER SERVICE IF EXISTS {service_name} RESUME')
        else:
            raise AttributeError('Invalid option for local_test')
        return None

    def remove_service(self, service_name:str):
        if self.local_test == 'astro_cli':
            raise NotImplementedError('Remove service via Astro CLI.')

        elif self.local_test == 'docker_desktop_k8s':
            from kubernetes import client, config
            from time import sleep
            config.load_kube_config()
            
            status = self.describe_service(service_name=service_name)
            for deployment in status['deployments'].keys():
                v1 = client.AppsV1Api()
                v1.delete_namespaced_deployment(name=deployment, namespace=service_name) #deployment['namespace'])
            for pod in status['pods'].keys():
                v1 = client.CoreV1Api()
                v1.delete_namespaced_pod(name=pod, namespace=service_name) # namespace=pod['namespace'])
            for service in status['services'].keys():
                v1 = client.CoreV1Api()
                v1.delete_namespaced_service(name=service, namespace=service_name) # namespace=service['namespace'])

            v1 = client.CoreV1Api()

            pods = v1.list_namespaced_pod(namespace=service_name).items
            if len(pods) > 0:
                pods_exist = True
            while pods_exist:
                pods_exist = False
                pods = v1.list_namespaced_pod(namespace=service_name).items
                if len(pods) > 0:
                    nameservice_exists = True
                sleep(2)
            
            v1.delete_namespace(name=service_name)

            nameservice_exists = True
            while nameservice_exists:
                nameservice_exists = False
                for namespace in v1.list_namespace().items:
                    if namespace.metadata.name == service_name:
                        sleep(2)
                        nameservice_exists = True
                        
        elif not self.local_test:    
            print(f'DROP SERVICE IF EXISTS {service_name}')
        else:
            raise AttributeError('Invalid option for local_test')
        
        return None

    def describe_service(self, service_name:str):
        response = {'pods': {}, 'services': {}, 'deployments': {}}

        if self.local_test == 'astro_cli':
            response['services'][service_name] = {'ingress_url': 'host.docker.internal:8001'}
            return response

        elif self.local_test == 'docker_desktop_k8s':
            from kubernetes import client, config
            config.load_kube_config()
                        
            v1 = client.CoreV1Api()
            
            for pod in v1.list_namespaced_pod(namespace=service_name).items: 
                response['pods'][pod.metadata.name] = {'ip': pod.status.pod_ip, 'namespace':pod.metadata.namespace}

            for service in v1.list_namespaced_service(namespace=service_name).items: 
                if service.metadata.name == service_name:
                    response['services'][service.metadata.name] = {'ingress_url': f'{service.status.load_balancer.ingress[0].hostname}:{service.spec.ports[0].target_port}'}

            v1 = client.AppsV1Api()
            for deployment in v1.list_namespaced_deployment(namespace=service_name).items: 
                response['deployments'][deployment.metadata.name] = {'metadata': deployment.metadata.namespace}
                
        elif not self.local_test:    
            #response = self.get_conn().cursor().execute(f'CALL SYSTEM$GET_SNOWSERVICE_STATUS({service_name}').fetchall()
            raise NotImplementedError()
        else:
            raise AttributeError('Invalid option for local_test')
        
        return response
            
    def get_runner_url(self, service_name: str): 

        services = self.describe_service(service_name=service_name)['services']

        if self.local_test == 'astro_cli' or self.local_test == 'docker_desktop_k8s':
            return services[service_name]['ingress_url']

        elif not self.local_test:    

            response = f"self.get_conn().cursor().execute(f'CALL SYSTEM$GET_SNOWSERVICE_STATUS({service_name}').fetchall()"

        else:
            raise AttributeError('Invalid option for local_test')

        
    def check_service(self, service_name:str):

        service_url = self.get_runner_url(service_name=service_name)
        response = requests.get(service_url)
        assert response.status_code == 200
        assert response.json() == "Pong."
        print(f'reponse is: {response}')