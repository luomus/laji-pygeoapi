from unittest.mock import patch, mock_open
import os

from scripts  import edit_configmaps

# run with:
# cd pygeoapi
# python -m pytest tests/test_edit_configmaps.py -v

# Test for get_kubernetes_info function
@patch("builtins.open", new_callable=mock_open, read_data="namespace-data")
@patch.dict(os.environ, {"KUBERNETES_SERVICE_HOST": "172.30.0.1", "KUBERNETES_SERVICE_PORT": "443"})
def test_get_kubernetes_info(mock_file):
    api_url, namespace = edit_configmaps.get_kubernetes_info()
    assert api_url == "https://172.30.0.1:443"
    assert namespace == "namespace-data"

# Test for delete_pod function
@patch("requests.delete")
def test_delete_pod(mock_delete):
    api_url = "https://172.30.0.1:443"
    namespace = "namespace-data"
    pod = "test-pod"
    headers = {"Authorization": "Bearer token-data"}
    ca_cert = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

    edit_configmaps.delete_pod(api_url, namespace, pod, headers, ca_cert)

    pod_url = f"{api_url}/api/v1/namespaces/{namespace}/pods/{pod}"
    mock_delete.assert_called_once_with(pod_url, headers=headers, verify=ca_cert)

# Test for update_configmap function
@patch("builtins.open", new_callable=mock_open, read_data="config-content")
@patch("requests.patch")
def test_update_configmap(mock_patch, mock_file):
    configmap_url = "https://172.30.0.1:443/api/v1/namespaces/namespace-data/configmaps/test-configmap"
    headers = {"Authorization": "Bearer token-data", "Content-Type": "application/json-patch+json"}
    ca_cert = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
    file_to_add = "/path/to/pygeoapi-config.yml"
    path = "/data/pygeoapi-config.yml"

    edit_configmaps.update_configmap(configmap_url, headers, ca_cert, file_to_add, path)

    patch_data = [{
        "op": "replace",
        "path": path,
        "value": "config-content"
    }]
    mock_patch.assert_called_once_with(configmap_url, headers=headers, json=patch_data, verify=ca_cert)

@patch("scripts.edit_configmaps.get_kubernetes_info", return_value=("https://kube.api", "default"))
@patch("scripts.edit_configmaps.update_configmap")
@patch("scripts.edit_configmaps.delete_pod")
@patch("builtins.open", new_callable=mock_open, read_data="fake-token")
@patch("requests.get")
@patch.dict(os.environ, {"CONFIG_MAP_NAME": "test-map", "SERVICE_NAME": "pygeoapi-branch"})
def test_update_and_restart(mock_requests_get, mock_open_token, mock_delete_pod, mock_update_configmap, mock_get_kube_info):
    # Fake pod response
    mock_requests_get.return_value.json.return_value = {
        "items": [
            {
                "metadata": {
                    "name": "pygeoapi-branch-123",
                    "labels": {
                        "io.kompose.service": "pygeoapi-branch"
                    }
                }
            }
        ]
    }

    # Run function
    edit_configmaps.update_and_restart("fake-config.yml", "fake-db.tinydb")

    # Assertions
    assert mock_update_configmap.call_count == 2
    mock_open_token.assert_any_call("/var/run/secrets/kubernetes.io/serviceaccount/token") # Check that token file was opened
    assert mock_open_token.call_count == 2     # Check that open was called twice (for .env and token)
    mock_delete_pod.assert_called_once_with("https://kube.api", "default", "pygeoapi-branch-123", {"Authorization": "Bearer fake-token"}, "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt") # Check that delete_pod was called with correct parameters
    mock_requests_get.assert_called_once() # Check that requests.get was called once
    assert mock_requests_get.call_args[0][0] == "https://kube.api/api/v1/namespaces/default/pods" # Check the URL called in requests.get
    assert "Authorization" in mock_requests_get.call_args[1]["headers"] # Check that headers were passed in requests.get
