import requests, os
from dotenv import load_dotenv

def get_kubernetes_info():
    '''
    This function retrieves the Kubernetes API URL and the current namespace.
    It reads the namespace from the Kubernetes service account token file,
    constructs the API URL using the Kubernetes service host and port from environment variables,
    and returns both the API URL and namespace.

    Returns:
    api_url (string)
    namespace (string)
    '''
    with open("/var/run/secrets/kubernetes.io/serviceaccount/namespace") as f:
        namespace = f.read().strip()

    api_url = "https://{}:{}".format(
        os.environ['KUBERNETES_SERVICE_HOST'],
        os.environ['KUBERNETES_SERVICE_PORT']
    )

    return api_url, namespace

def update_configmap(pygeoapi_config_out):
    '''
    This function updates a Kubernetes ConfigMap with new data from a given file and restarts the related pods.
    1. Retrieves the Kubernetes API URL and namespace.
    2. Reads the CA certificate and service account token.
    3. Reads the content of the pygeoapi config file.
    4. Prepares a JSON patch request to update the ConfigMap.
    5. Sends a PATCH request to the Kubernetes API to update the ConfigMap.
    6. Retrieves the list of pods and identifies the ones related to the deployment.
    7. Deletes the identified pods to trigger a restart with the updated ConfigMap.
    '''

    print("Updating configmap...")
    
    api_url, namespace = get_kubernetes_info()

    # Get branch
    load_dotenv()
    branch = os.getenv('BRANCH')
    if branch is None:
        raise ValueError("BRANCH environment variable is not set")
    
    # Read the CA certificate
    ca_cert = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

    # Read the service account token
    with open("/var/run/secrets/kubernetes.io/serviceaccount/token") as f:
        token = f.read()

    # Read the content of the config file to update
    with open(pygeoapi_config_out, "r") as f:
        pygeoapi_config_content = f.read()       

    # Prepare the patch data to replace the config map    
    data = [{
        "op": "replace",
        "path": "/data/pygeoapi-config.yml",
        "value": pygeoapi_config_content
    }]

    # Update the config map
    headers = {"Authorization": "Bearer {}".format(token), "Content-Type": "application/json-patch+json"}
    configmap_url = f"{api_url}/api/v1/namespaces/{namespace}/configmaps/pygeoapi-config-{branch}"
    r = requests.patch(configmap_url, headers=headers, json=data, verify=ca_cert)

    # Print possible errors
    if r.status_code != 200:
        print(f"Failed to update configmap: {r.status_code} - {r.text}")
        r.raise_for_status()

    # find the pods we want to restart
    headers = {"Authorization": "Bearer {}".format(token)}
    pods_url = f"{api_url}/api/v1/namespaces/{namespace}/pods"
    print(pods_url)

    data = requests.get(pods_url, headers=headers, verify=ca_cert).json()
    items = data["items"]
    print(items)

    service_name = "pygeoapi-"+branch
    try:
        # Find pods with the label io.kompose.service set to pygeoapi-dev
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
        pod_url = f"{api_url}/api/v1/namespaces/{namespace}/pods/{pod}"
        print(pod_url)
        try:
            delete_response  = requests.delete(pod_url, headers=headers, verify=ca_cert)
            delete_response.raise_for_status()
            print(f"Pod {pod} updated")
        except requests.exceptions.RequestException as e:
            print(f"Error deleting pod {pod}: {e}")

    