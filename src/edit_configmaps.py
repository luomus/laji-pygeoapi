import requests

def update_configmap(pygeoapi_config_out):
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
    r = requests.patch("https://api.2.rahti.csc.fi:6443/api/v1/namespaces/laji-pygeoapi/configmaps/pygeoapi-config", headers=headers, json=data)

    # find the pods we want to restart
    headers = {"Authorization": "Bearer {}".format(token)}
    data = requests.get("https://api.2.rahti.csc.fi:6443/api/v1/namespaces/laji-pygeoapi/pods", headers=headers).json()
    items = data["items"]

    deploymentconfig_name = "pygeoapi"
    target_pods = [i["metadata"]["name"] for i in items if "labels" in i["metadata"] and "deploymentconfig" in i["metadata"]["labels"] and i["metadata"]["labels"]["deploymentconfig"] == deploymentconfig_name]

    # restart the pods by deleting them
    for pod in target_pods:
        a = requests.delete("https://api.2.rahti.csc.fi:6443/api/v1/namespaces/laji-pygeoapi/pods/{}".format(pod), headers=headers)