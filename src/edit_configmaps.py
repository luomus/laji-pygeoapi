import requests, os

def get_kubernetes_info():
    with open("/var/run/secrets/kubernetes.io/serviceaccount/namespace") as f:
        namespace = f.read().strip()

    api_url = "https://{}:{}".format(
        os.environ['KUBERNETES_SERVICE_HOST'],
        os.environ['KUBERNETES_SERVICE_PORT']
    )

    return api_url, namespace

def update_configmap(pygeoapi_config_out):

    api_url, namespace = get_kubernetes_info()


    with open("/var/run/secrets/kubernetes.io/serviceaccount/token") as f:
        token = f.read()

    with open(pygeoapi_config_out, "r") as f:
        file_content = f.read()       

    # replace the config map    
    data = [{
        "op": "replace",
        "path": "/data/pygeoapi-config.yml",
        "value": file_content
    }]

    headers = {"Authorization": "Bearer {}".format(token), "Content-Type": "application/json-patch+json"}
    r = requests.patch(f"{api_url}/api/v1/namespaces/{namespace}/configmaps/pygeoapi-config", headers=headers, json=data)

    # find the pods we want to restart
    headers = {"Authorization": "Bearer {}".format(token)}
    data = requests.get(f"{api_url}/api/v1/namespaces/{namespace}/pods", headers=headers).json()
    items = data["items"]

    deploymentconfig_name = "pygeoapi"
    target_pods = [i["metadata"]["name"] for i in items if "labels" in i["metadata"] and "deploymentconfig" in i["metadata"]["labels"] and i["metadata"]["labels"]["deploymentconfig"] == deploymentconfig_name]

    # restart the pods by deleting them
    for pod in target_pods:
        a = requests.delete(f"{api_url}/api/v1/namespaces/{namespace}/pods/{pod}", headers=headers)