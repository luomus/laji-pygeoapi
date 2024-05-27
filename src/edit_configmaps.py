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

    # Read the CA certificate
    ca_cert = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

    # Read the service account token
    with open("/var/run/secrets/kubernetes.io/serviceaccount/token") as f:
        token = f.read()

    # Read the content of the config file to update
    with open(pygeoapi_config_out, "r") as f:
        file_content = f.read()       

    # Prepare the patch data to replace the config map    
    data = [{
        "op": "replace",
        "path": "/data/pygeoapi-config.yml",
        "value": file_content
    }]

   
    # Update the config map
    headers = {"Authorization": "Bearer {}".format(token), "Content-Type": "application/json-patch+json"}
    configmap_url = f"{api_url}/api/v1/namespaces/{namespace}/configmaps/pygeoapi-config"
    r = requests.patch(configmap_url, headers=headers, json=data, verify=ca_cert)

    # find the pods we want to restart
    headers = {"Authorization": "Bearer {}".format(token)}
    pods_url = f"{api_url}/api/v1/namespaces/{namespace}/pods"
    data = requests.get(pods_url, headers=headers, verify=ca_cert).json()
    items = data["items"]

    deploymentconfig_name = "pygeoapi"
    target_pods = [i["metadata"]["name"] for i in items if "labels" in i["metadata"] and "deploymentconfig" in i["metadata"]["labels"] and i["metadata"]["labels"]["deploymentconfig"] == deploymentconfig_name]

    # restart the pods by deleting them
    for pod in target_pods:
        pod_url = f"{api_url}/api/v1/namespaces/{namespace}/pods/{pod}"
        a = requests.delete(pod_url, headers=headers, verify=ca_cert)
        a.raise_for_status()