import os
import json
from typing import Any
import pandas as pd
from airflow.models.xcom import BaseXCom
from airflow.models import XCom
from airflow.utils.json import XComDecoder, XComEncoder
from pathlib import Path

ENCODING = "utf-8"

class LocalFileXComBackend(BaseXCom):
    """
    Custom XCom backend that stores XComs in the Local Filesystem. JSON serializable objects 
    are stored as json text files.  Pandas dataframes are stored as parquet files.
    Requires a specified xcom directory.
    Enable with these env variables:
        AIRFLOW__CORE__XCOM_BACKEND=backends.local.LocalFileXComBackend
        AIRFLOW__CORE__XCOM_LOCALFILE_DIR=<local directory name>
    """
    @staticmethod
    def check_xcom_dir():
        assert os.getenv('AIRFLOW__CORE__XCOM_LOCALFILE_DIR'), 'AIRFLOW__CORE__XCOM_LOCALFILE_DIR environment variable not set'
        xcom_dir: Path = Path(os.getenv('AIRFLOW__CORE__XCOM_LOCALFILE_DIR'))
        if xcom_dir.is_dir() and os.access(xcom_dir, os.R_OK | os.W_OK | os.X_OK):
            return xcom_dir
        else:
            raise NotADirectoryError(f'AIRFLOW__CORE__XCOM_LOCALFILE_DIR directory {xcom_dir} is not found or missing permissions.')
    @staticmethod
    def serialize_value(
        value: Any, 
        key: str, 
        dag_id: str, 
        task_id: str, 
        run_id: str, 
        map_index: int = -1
    ) -> bytes:
        """
        Writes the value to AIRFLOW__CORE__XCOM_LOCALFILE_DIR and returns the file key. The key is used to retrieve the value later.
        The local path is: <AIRFLOW__CORE__XCOM_LOCALFILE_DIR>/<dag_id>/<task_id>/<run_id>/<key>.json or .parquet
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
        :return: The file key byte encoded string.
        :rtype: str
        """

        xcom_dir: Path = LocalFileXComBackend.check_xcom_dir()
        output_dir: Path = xcom_dir.joinpath(f"{dag_id}/{task_id}/{run_id}/")
        if not output_dir.is_dir():
            try:
                output_dir.mkdir(parents=True)
            except Exception as e: 
                raise NotADirectoryError(f'Specified XCOM output directory {output_dir} does not exist and cannot be created.')

        elif not os.access(output_dir, os.R_OK | os.W_OK | os.X_OK):
            raise NotADirectoryError(f'Specified XCOM output directory {output_dir} exist but is missing permissions.')

        if isinstance(value, pd.DataFrame):
            file_path = output_dir.joinpath(f'{key}.parquet')
            value.to_parquet(file_path)
            
        else:
            file_path = output_dir.joinpath(f'{key}.json')
            try:
                file_path.write_bytes(json.dumps(value, cls=XComEncoder).encode("UTF-8"))
            except: 
                raise
        
        return BaseXCom.serialize_value(file_path.as_posix())


    @staticmethod
    def deserialize_value(result: XCom) -> Any:
        """
        Reads the value from AIRFLOW__CORE__XCOM_LOCALFILE_DIR using the file_path.
        The file path is assumed to be: <AIRFLOW__CORE__XCOM_LOCALFILE_DIR>/<dag_id>/<task_id>/<run_id>/<key>.json or .parquet
        :param file_path: The FQ file path.
        :type file_path: Byte-encoded str
        :return: The deserialized value.
        :rtype: Any
        """

        # first, decode the key
        file_path = Path(BaseXCom.deserialize_value(result))
        #file_path = Path(json.loads(file_path.decode("UTF-8"), cls=XComDecoder, object_hook=object_hook))
            
        assert file_path.open(), f'XCOM file at {file_path.as_posix()} cannot be opened.'

        if file_path.suffix == '.parquet':
            return pd.read_parquet(file_path)

        elif file_path.suffix == '.json':
            return json.loads(file_path.read_bytes())

        else:
            raise ValueError(f"XCOM file output must be .json or .parquet.  Found {file_path.suffix()}")  


os.environ['AIRFLOW__CORE__XCOM_LOCALFILE_DIR']='/Users/michaelgregory/Documents/Astronomer/code/demos/astronomer-providers/astronomer/providers/core/xcom_backends/local'
x=LocalFileXComBackend()
value={"a": 1, "b": 1}, {"a": 2, "b": 4}, {"a": 3, "b": 9}
value=pd.DataFrame(value)
file_path = x.serialize_value(value=value, dag_id='testdag', task_id='testtask', run_id='testrun', key='testkey')
value= x.deserialize_value(file_path=file_path)