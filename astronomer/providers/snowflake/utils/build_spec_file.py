from __future__ import annotations

def create_k8s_spec(
    service_name:str, 
    runner_endpoint:str, 
    runner_port:int, 
    local_test:str | None = None
    ):

    if runner_port and not isinstance(runner_port, int):
        raise AttributeError('runner_port must be an integer.')
    
    runner_endpoint = runner_endpoint or 'runner-endpoint'
    runner_port = runner_port or 8001

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
                        'image': 'snowservices-runner:latest', 
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
                        {'name': 'xcom', 'source': 'local'}, 
                        #{'name': 'xcom', 'hostPath': {'path': f'{os.getcwd()}/xcom', 'type': 'Directory'}},
                    ], 
                }
            }
        }
    }

    if local_test:
        return [lb_spec, deployment_spec]
    else:
        return {'spec': {'container': deployment_spec['spec']['template']['spec']['containers'], 
                         'endpoint': lb_spec['spec']['ports'],
                         'volume': deployment_spec['spec']['template']['spec']['volumes']}}
