from __future__ import annotations
import os

def create_k8s_spec(
    service_name:str, 
    runner_endpoint:str = 'runner-endpoint', 
    runner_port:int = 8001, 
    runner_image: str = 'snowservices-runner:latest',
    local_test:str | None = None
    ):

    if runner_port and not isinstance(runner_port, int):
        raise AttributeError('runner_port must be an integer.')
    
    #For testing in local (aka Docker Desktop Kubernetes) we will create a loadbalancer and full deployment spec.
    lb_spec = {
        'apiVersion': 'v1', 
        'kind': 'Service', 
        'metadata': {
            'name': service_name,
            }, 
        'spec': {
            'ports': [{'name': runner_endpoint, 
                    'port': runner_port, 
                    'protocol': 'TCP', 
                    'targetPort': runner_port
                    }], 
            'selector': {'app.kubernetes.io/name': 'snowservice-runner'}, 
            'type': 'LoadBalancer'
            }, 
    }

    deployment_spec = {
        'apiVersion': 'apps/v1', 
        'kind': 'Deployment', 
        'metadata': {
            'labels': {'app.kubernetes.io/name': 'snowservice-runner'}, 
            'name': 'snowservice-runner',
        }, 
        'spec': {
            'selector': {'matchLabels': {'app.kubernetes.io/name': 'snowservice-runner'}}, 
            'template': {
                'metadata': {
                    'labels': {'app.kubernetes.io/name': 'snowservice-runner'}}, 
                'spec': {
                    'containers': [{
                        'image': runner_image, 
                        'imagePullPolicy': 'IfNotPresent', 
                        'name': 'snowservice-runner', 
                        'args': [], 
                        'command': [], 
                        'env': [{'name': 'AIRFLOW__CORE__XCOM_BACKEND', 
                                 'value': 'backends.local.localfile_xcom_backend.LocalFileXComBackend'},
                                {'name': 'AIRFLOW__CORE__XCOM_LOCALFILE_DIR', 
                                 'value': '/xcom'}], 
                        'ports': [{'containerPort': runner_port, 'protocol': 'TCP'}], 
                        'readinessProbe': {'tcpSocket': {'port': runner_port}}, 
                        'resources': {
                            'requests': {'memory': '500Mi', 'cpu': '500m', 'nvidia.com/gpu': '0'}, 
                            'limits': {'memory': '1Gi', 'cpu': '500m', 'nvidia.com/gpu': '0'}}, 
                        'volumeMounts': [
                            {'mountPath': '/xcom', 
                            'name': 'xcom'}, 
                        ]
                    }], 
                    'volumes': [
                        # {'name': 'xcom', 'source': 'local'}, 
                        {'name': 'xcom', 'hostPath': {'path': f'{os.getcwd()}/xcom', 'type': 'DirectoryOrCreate'}},
                        # {'name': 'xcom', 'hostPath': {'path': './xcom', 'type': 'DirectoryOrCreate'}},
                    ], 
                }
            }
        }
    }
    
    snowservices_spec = {
        'endpoints': 
        [
            {
                'name': runner_endpoint, 
                'port': runner_port, 
            },    
        ],
        'containers': 
        [
            {
                'name': 'snowservice-runner', 
                'image': runner_image,
                'command': ["gunicorn", "api:app", "--bind", "0.0.0.0:8001", "-w", "1", "-k", "uvicorn.workers.UvicornWorker", "--timeout", "0"], 
                'env': [
                    'AIRFLOW__CORE__LOAD_EXAMPLES:False',
                    'AIRFLOW__CORE__ENABLE_XCOM_PICKLING:True',
                    'PYTHONUNBUFFERED:1',
                    'AIRFLOW__CORE__XCOM_BACKEND:backends.local.localfile_xcom_backend.LocalFileXComBackend',
                    'AIRFLOW__CORE__XCOM_LOCALFILE_DIR:/xcom',
                ]
            }
        ],
        'volumes': 
        [
            {'name': 'xcom', 'source': 'local'},  
        ]
    }

    if local_test:
        return [lb_spec, deployment_spec]
    else:
        return [snowservices_spec]
