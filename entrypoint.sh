#!/bin/bash

# Step 1: Run the Python script main.py
python3 main.py

# Step 2: Update the config file and store it into the configmap
pod_name=$(kubectl get pods -l io.kompose.service=pygeoapi -o jsonpath='{.items[0].metadata.name}')
kubectl cp pygeoapi-config.yml $pod_name:/pygeoapi/local.config.yml

# Step 3: Restart the pod "pygeoapi-10"
kubectl delete pod $pod_name
