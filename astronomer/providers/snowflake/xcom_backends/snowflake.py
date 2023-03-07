import tempfile
from pathlib import Path
from urllib import parse as parser
import os
import json
from typing import TYPE_CHECKING, Any
import pandas as pd
import numpy as np
import warnings
from airflow.models.xcom import BaseXCom
if TYPE_CHECKING:
    from airflow.models.xcom import XCom

from astronomer.providers.snowflake.hooks.snowflake import SnowflakeHook
    
_ENCODING = "utf-8"
_SNOWFLAKE_VARIANT_SIZE_LIMIT = 16777216
_SUPPORTED_FILE_TYPES = ['json', 'parquet', 'np', 'bin']

##TODO: map_index
##TODO: multiple xcom return
##TODO: troubleshoot tuple serialization returns list


class SnowflakeXComBackend(BaseXCom):
    """
    Custom XCom backend that stores XComs in the Snowflake. JSON serializable objects 
    are stored as tables.  Dataframes (pandas and Snowpark) are stored as parquet files in a stage.
    Requires a specified xcom directory.
    Enable with these env variables:
        AIRFLOW__CORE__XCOM_BACKEND=astronomer.providers.snowflake.xcom_backends.snowflake.SnowflakeXComBackend
        AIRFLOW__CORE__XCOM_SNOWFLAKE_TABLE=<DB.SCHEMA.TABLE>
        AIRFLOW__CORE__XCOM_SNOWFLAKE_STAGE=<DB.SCHEMA.STAGE>
        AIRFLOW__CORE__XCOM_SNOWFLAKE_CONN_NAME=<CONN_NAME>  #default 'snowflake_default'
    """
    def __init__(self):
        snowflake_conn_id = SnowflakeXComBackend.check_xcom_conn()
        self.hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
        
        self.snowflake_xcom_table = SnowflakeXComBackend.check_xcom_table(self)
        self.snowflake_xcom_stage = SnowflakeXComBackend.check_xcom_stage(self)
        
        # super().__init__()

    @staticmethod
    def check_xcom_conn():
        try: 
            snowflake_conn_id = os.getenv('AIRFLOW__CORE__XCOM_SNOWFLAKE_CONN_NAME')
        except:
            "AIRFLOW__CORE__XCOM_SNOWFLAKE_CONN_NAME environment variable not set. Using default value 'snowflake_default'"
            return 'snowflake_default'
        # SnowflakeHook(snowflake_conn_id=snowflake_conn_id).test_connection()
        return snowflake_conn_id

    @staticmethod
    def check_xcom_table(self):
        try: 
            snowflake_xcom_table = os.getenv('AIRFLOW__CORE__XCOM_SNOWFLAKE_TABLE')
        except: 
            AttributeError('AIRFLOW__CORE__XCOM_SNOWFLAKE_TABLE environment variable not set')
        
        assert len(snowflake_xcom_table.split('.')) == 3, "AIRFLOW__CORE__XCOM_SNOWFLAKE_TABLE is not a fully-qualified Snowflake table objet"

        try:
            self.hook.run('DESCRIBE TABLE '+snowflake_xcom_table)
        except Exception as e:
            if 'does not exist or not authorized' in e.msg:
                try: 
                    warnings.warn(f'XCOM table {snowflake_xcom_table} does not exist or not authorized. Attempting to create it.')
                    SnowflakeXComBackend.create_xcom_table(self, snowflake_xcom_table=snowflake_xcom_table)
                except: 
                    raise AttributeError(f'XCOM table {snowflake_xcom_table} does not exist and could not be created.')

        return snowflake_xcom_table

    @staticmethod
    def check_xcom_stage(self):
        try: 
            snowflake_xcom_stage = os.getenv('AIRFLOW__CORE__XCOM_SNOWFLAKE_STAGE')
        except: 
            AttributeError('AIRFLOW__CORE__XCOM_SNOWFLAKE_STAGE environment variable not set')
        
        assert len(snowflake_xcom_stage.split('.')) == 3, "AIRFLOW__CORE__XCOM_SNOWFLAKE_STAGE is not a fully-qualified Snowflake stage objet"

        try:
            self.hook.run('DESCRIBE STAGE '+snowflake_xcom_stage)
        except Exception as e:
            if 'does not exist or not authorized' in e.msg:
                try: 
                    warnings.warn(f'XCOM stage {snowflake_xcom_stage} does not exist or not autorized. Attempting to create it.')
                    SnowflakeXComBackend.create_xcom_stage(self, snowflake_xcom_stage=snowflake_xcom_stage)
                except:
                    raise AttributeError(f'XCOM stage {snowflake_xcom_stage} does not exist and could not be created.')

        return snowflake_xcom_stage

    @staticmethod
    def create_xcom_stage(self, snowflake_xcom_stage):
        self.hook.run(f'CREATE OR REPLACE STAGE {snowflake_xcom_stage}')
    
    @staticmethod
    def create_xcom_table(self, snowflake_xcom_table):
        self.hook.run(f'''
            CREATE OR REPLACE TABLE {snowflake_xcom_table} 
                (
                    key varchar PRIMARY KEY, 
                    dag_id varchar NOT NULL, 
                    task_id varchar NOT NULL, 
                    run_id varchar NOT NULL,
                    map_index integer NOT NULL,
                    value variant
                ) 
            '''
            )

    def _serialize(self,
        value: Any, 
        key: str, 
        dag_id: str, 
        task_id: str, 
        run_id: str,
        map_index: int,
        ) -> str:
        """
        Writes JSON serializable content as VARIANT column in AIRFLOW__CORE__XCOM_SNOWFLAKE_TABLE and 
        writes non-serializable content as files in AIRFLOW__CORE__XCOM_SNOWFLAKE_STAGE. 
        returns a URI for the table or file to be serialized in the Airflow XCOM DB. 

        The URI format is: 
            snowflake://<ACCOUNT>.<REGION>/<DATABASE>/<SCHEMA>?table=<TABLE>&key=<KEY>
            or
            snowflake://<ACCOUNT>.<REGION>/<DATABASE>/<SCHEMA>?stage=<STAGE>&key=<FILE_PATH>
        
        :param value: The value to serialize.
        :type value: Any
        :param key: The key to use for the xcom output (ie. filename)
        :type: key: str
        :param dag_id: DAG id
        :type dag_id: str
        :param task_id: Task id
        :type task_id: str
        :param run_id: DAG run id
        :type run_id: str
        :param map_index: 
        :type map_index: int
        :return: The byte encoded uri string.
        :rtype: str
        """

        conn_params = self.hook._get_conn_params()

        base_uri = f"snowflake://{conn_params['account']}.{conn_params['region']}?"

        try:
            json_value = json.dumps(value)

            if len(json_value.encode(_ENCODING)) < _SNOWFLAKE_VARIANT_SIZE_LIMIT:

                self.hook.run(f"""
                    MERGE INTO {self.snowflake_xcom_table} tab1
                    USING (SELECT
                                '{dag_id}' AS dag_id, 
                                '{task_id}' AS task_id, 
                                '{run_id}' AS run_id, 
                                '{map_index}' AS map_index,
                                '{key}' AS key,  
                                parse_json('{json_value}') AS value) tab2
                    ON tab1.dag_id = tab2.dag_id 
                        AND tab1.task_id = tab2.task_id
                        AND tab1.run_id = tab2.run_id
                        AND tab1.key = tab2.key
                        AND tab1.map_index = tab2.map_index
                    WHEN MATCHED THEN UPDATE SET tab1.value = tab2.value 
                    WHEN NOT MATCHED THEN INSERT (dag_id, task_id, run_id, map_index, key, value)
                          VALUES (tab2.dag_id, tab2.task_id, tab2.run_id, tab2.map_index, tab2.key, tab2.value);
                """)

                # # self.hook.run(
                # print(f"""INSERT INTO {self.snowflake_xcom_table} 
                #         (dag_id, key, map_index, run_id, task_id, value)
                #         SELECT
                #             '{dag_id}', 
                #             '{key}', 
                #             '{map_index}', 
                #             '{run_id}', 
                #             '{task_id}', 
                #             parse_json('{json_value}')
                #     """)
                return base_uri + f"&table={self.snowflake_xcom_table}&key={dag_id}/{task_id}/{run_id}/{key}"
                
            else:
                warnings.warn(f'XCOM value size exceeds Snowflake cell size limit {_SNOWFLAKE_VARIANT_SIZE_LIMIT}. Resorting to file/stage serialization.')
                
        except Exception as e:
            if isinstance(e, TypeError) and 'not JSON serializable' in e.args[0]:
                warnings.warn('Attempting to serialize XCOM value but it is not JSON serializable.  Will try file/stage serialization.')
            else:
                raise e()

        with tempfile.TemporaryDirectory() as td:
            if 'json_value' in locals():
                temp_file = Path(f'{td}/{key}.json')
                _ = temp_file.write_text(json_value, encoding=_ENCODING)
            
            elif isinstance(value, Path) and Path(value).is_file() and os.access(value, os.R_OK):
                temp_file = Path(value)

            elif isinstance(value, (pd.DataFrame, pd.core.series.Series)):
                temp_file = Path(f'{td}/{key}.parquet')
                pd.DataFrame(value).to_parquet(temp_file)

            elif isinstance(value, np.ndarray):
                temp_file = Path(f'{td}/{key}.np')
                temp_file.write_bytes(value.dumps())

            else:
                try:
                    temp_file = Path(f'{td}/{key}.bin')
                    _ = temp_file.write_bytes(value)
                except:
                    raise AttributeError(f'Could not serialize object of type {type(value)}')
            print(temp_file)
            
            {dag_id}/{task_id}/{run_id}/{temp_file.name}
            
            # self.hook.run(
            print(f"PUT file://{temp_file} @{self.snowflake_xcom_stage}/{dag_id}/{task_id}/{run_id}/ AUTO_COMPRESS = TRUE")

            return base_uri + f"&stage={self.snowflake_xcom_stage}&key={dag_id}/{task_id}/{run_id}/{temp_file.name}"

    def _deserialize(self, uri: str) -> Any:
        """
        Reads the value from Snowflake XCOM backend given a URI.
        
        The URI format is: 
            snowflake://<ACCOUNT>.<REGION>/<DATABASE>/<SCHEMA>?table=<TABLE>&key=<KEY>
            or
            snowflake://<ACCOUNT>.<REGION>/<DATABASE>/<SCHEMA>?stage=<STAGE>&key=<FILE_PATH>
        
        :param uri: The XCOM uri.
        :type uri: str
        :return: The deserialized value.
        :rtype: Any
        """

        # uri = base_uri + f"&table={self.snowflake_xcom_table}&key={key}"
        # uri = base_uri + f"&stage={self.snowflake_xcom_stage}&key={dag_id}/{task_id}/{run_id}/{temp_file.name}"

        assert parser.splittype(uri)[0] == 'snowflake', "Unsupported scheme in XCOM uri."
        
        try: 
            account, region = parser.urlsplit(uri).netloc.split('.')
            uri_query = parser.parse_qs(uri)
            xcom_stage = uri_query.get('stage') 
            xcom_table = uri_query.get('table')
            xcom_key = uri_query.get('key') 
        except: 
            raise AttributeError('Failed to parse XCOM URI.')

        self.hook.account = account
        self.hook.region = region

        xcom_key = xcom_key[0]

        if xcom_stage:
            xcom_stage = xcom_stage[0]

            with tempfile.TemporaryDirectory() as td:
                self.hook.run(f"GET @{self.snowflake_xcom_stage}/{xcom_key} file://{td}")

                temp_file = Path(td).joinpath(Path(xcom_key).name)
                temp_file_type = temp_file.as_posix().split('.')[-1]

                assert temp_file_type in _SUPPORTED_FILE_TYPES, f'XCOM file type {temp_file_type} is not supported.'

                if temp_file_type == 'parquet':
                    return pd.read_parquet(temp_file)
                elif temp_file_type == 'np':
                    return np.load(temp_file, allow_pickle=True)
                elif temp_file_type == 'json':
                    return json.loads(temp_file.read_text())
                elif temp_file_type == 'bin':
                    return temp_file.read_bytes()
            
        elif xcom_table:
            xcom_table = xcom_table[0]
            xcom_cols = xcom_key.split('/')

            value = self.hook.get_records(f""" 
                                    SELECT VALUE FROM {xcom_table} \
                                    WHERE dag_id = '{xcom_cols[0]}' \
                                    AND task_id = '{xcom_cols[1]}' \
                                    AND run_id = '{xcom_cols[2]}';\
                                    """)

            return json.loads(value[0][0])

        else:
            raise AttributeError('Neither stage or table provided in XCOM URI.')

    @staticmethod
    def serialize_value(
        value: Any, 
        key: str, 
        dag_id: str, 
        task_id: str, 
        run_id: str, 
        map_index: int = -1,
        **kwargs
    ) -> Any:
        """
        Custom Xcom Wrapper to serialize to local files and returns the file key to Xcom.
        
        :param value: The value to serialize.
        :type value: Any
        :param key: The key to use for the xcom output
        :type: key: str
        :param dag_id: DAG id
        :type dag_id: str
        :param task_id: Task id
        :type task_id: str
        :param run_id: DAG run id
        :type run_id: str
        :return: The URI and key in byte encoded string.
        :rtype: str
        """
        if value:
            uri: str = SnowflakeXComBackend._serialize(value=value, key=key, dag_id=dag_id, task_id=task_id, run_id=run_id, map_index=map_index)
        
        return BaseXCom.serialize_value(uri)

    @staticmethod
    def deserialize_value(result: "XCom") -> Any:
        """
        Deserialize the result of xcom_pull before passing the result to the next task.
        :param result: Xcom result
        :return: 
        """

        uri = BaseXCom.deserialize_value(result)
        
        return SnowflakeXComBackend._deserialize(uri)
