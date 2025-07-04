import requests, os
from dotenv import load_dotenv

def get_kubernetes_info():
    '''
    This function retrieves the Kubernetes API URL and the current namespace.
    It reads the namespace from the Kubernetes service account token file,
    constructs the API URL using the Kubernetes service host and port from environment variables,
    and returns both the API URL and namespace.

    Returns:
    tuple: (api_url (string), namespace (str))
    '''
    with open("/var/run/secrets/kubernetes.io/serviceaccount/namespace") as f:
        namespace = f.read().strip()

    api_url = "https://{}:{}".format(
        os.environ['KUBERNETES_SERVICE_HOST'],
        os.environ['KUBERNETES_SERVICE_PORT']
    )

    return api_url, namespace

def delete_pod(api_url, namespace, pod, headers, ca_cert):
    '''
    Deletes a Kubernetes/Openshift pod to trigger a restart.

    Parameters:
    - api_url (str): The base URL for the Kubernetes API.
    - namespace (str): The Kubernetes namespace where the pod resides.
    - pod (str): The name of the pod to delete.
    - headers (dict): The headers to include in the request.
    - ca_cert (str): The path to the CA certificate for SSL verification.
    '''
        
    pod_url = f"{api_url}/api/v1/namespaces/{namespace}/pods/{pod}"
    try:
        delete_response  = requests.delete(pod_url, headers=headers, verify=ca_cert)
        delete_response.raise_for_status()
        print(f"Pod {pod} restarted")
    except requests.exceptions.RequestException as e:
        print(f"Error deleting pod {pod}: {e}")


def update_configmap(configmap_url, headers, ca_cert, file_to_add, path):
    '''
    Updates a specific path in a Kubernetes ConfigMap using a JSON patch.

    Parameters:
    - configmap_url (str): The URL of the ConfigMap to update.
    - headers (dict): The authorization headers to include in the request.
    - ca_cert (str): The path to the CA certificate for SSL verification.
    - file_to_add (str): The file whose contents will replace the data at the specified path.
    - path (str): The path within the ConfigMap to update.
    '''
    # Read the content of the config file to update
    with open(file_to_add, "r") as f:
        file_content = f.read()       

    # Prepare the patch data to replace the config map    
    data = [{
        "op": "replace",
        "path": path,
        "value": file_content
    }]

    r = requests.patch(configmap_url, headers=headers, json=data, verify=ca_cert)

    # Print possible errors
    if r.status_code != 200:
        print(f"Failed to update pygeoapi config configmap: {r.status_code} - {r.text}")
        r.raise_for_status()

def update_and_restart(pygeoapi_config_out, metadata_db_path):
    '''
    Updates a Kubernetes ConfigMap with new configuration data and restarts related pods.

    Steps:
    1. Retrieves the Kubernetes API URL and namespace.
    2. Loads environment variables and reads the service account token.
    3. Updates the ConfigMap with new configuration data.
    4. Identifies the relevant pods by label and deletes them to trigger a restart.

    Parameters:
    pygeoapi_config_out (str): The path to the pygeoapi config file
    metadata_db_path (str): The path to the metadata database
    '''

    print("Updating configmap...")
    
    api_url, namespace = get_kubernetes_info()

    load_dotenv()
    config_map_name = os.getenv("CONFIG_MAP_NAME")
    if config_map_name is None:
        raise ValueError("CONFIG_MAP_NAME environment variable is not set")

    service_name = os.getenv("SERVICE_NAME")
    if service_name is None:
        raise ValueError("SERVICE_NAME environment variable is not set")

    
    # Read the CA certificate
    ca_cert = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

    # Read the service account token
    with open("/var/run/secrets/kubernetes.io/serviceaccount/token") as f:
        token = f.read()

    # Update the config map
    headers = {"Authorization": "Bearer {}".format(token), "Content-Type": "application/json-patch+json"}
    configmap_url = f"{api_url}/api/v1/namespaces/{namespace}/configmaps/{config_map_name}"

    update_configmap(configmap_url, headers, ca_cert, pygeoapi_config_out, "/data/pygeoapi-config.yml")
    update_configmap(configmap_url, headers, ca_cert, metadata_db_path, "/data/catalogue.tinydb")



    # find the pods we want to restart
    headers = {"Authorization": "Bearer {}".format(token)}
    pods_url = f"{api_url}/api/v1/namespaces/{namespace}/pods"

    data = requests.get(pods_url, headers=headers, verify=ca_cert).json()
    items = data["items"]

    try:
        # Find pods with the label io.kompose.service set to pygeoapi-branch
        target_pods = [
            item["metadata"]["name"]
            for item in items
            if "labels" in item["metadata"] and item["metadata"]["labels"].get("io.kompose.service") == service_name
        ]
    except KeyError as e:
        print(f"KeyError: {e} - Possibly missing key in item metadata.")
    except Exception as e:
        print(f"Error finding deployments: {e}")
        print(f"Items: {str(items)}")

    if not target_pods:
        print(f"No pods found for deployment: {service_name}")
        return

    # restart the pods by deleting them
    for pod in target_pods:
        delete_pod(api_url, namespace, pod, headers, ca_cert)

    