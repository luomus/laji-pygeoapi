
# OpenShift Deployment Instructions
Step-by-Step Guide to deploy this API to OpenShift

## 1. Log in to OpenShift

```
oc login https://your-openshift-cluster-url
```

## 2. Set the Project/NAMESPACE

Ensure you are in the correct project/namespace. You can also create a new one if necessary.

```
oc project your-project-name
```

## 3. Fill in the Environment Values

Create an env file (for example test.env) and fill in the values, you can take example.env as an example. If there is
already a running version in the Openshift, make sure you use the same values especially for the database credentials.
The database credentials, "ACCESS_TOKEN" and "ACCESS_EMAIL" need to be base64 encoded. You
can get the encoded value with this command:

```
echo -n <value to be encoded> | base64
```

## 4. Process the Template

Process the template and create a list of objects.

```
oc process -f template.yaml --param-file=test.env > processed-template.yaml
```

If you are deploying the API for the first time, process also the pygeoapi config template. Later on the config
shouldn't be updated manually since it can cause errors for the API.

```
oc process -f pygeoapi-config-template.yaml --param-file=test.env --ignore-unknown-parameters=true > processed-config-template.yaml
```


## 5. Add Objects from the Processed Template

```
oc apply -f processed-config-template.yaml # if deploying the API for the first time
oc apply -f processed-template.yaml
```

## 6. Verify CronJob

If you are deploying the app for the first time or need to check if the CronJob is working, change the schedule of the
CronJob to ```schedule:  "*/1 * * * *"``` for example so that it runs the job every minute. After the job has started running,
you can change the schedule back to normal.

## 7. Wait for Pod Completion

Wait for the pod ```python-scripts-<BRANCH>-xxxx``` to complete its tasks. If ```"PAGES"``` environmental variable is a big number or ```"all"```, it can take some time. You can check the logs to see it's progressing. 

## 8. Create Database Tables for Request Log (not required if authentication is not enabled)

Navigate to the pod that is running the pygeoapi app and open the terminal. Create tables with the command

```
flask --app src.app db upgrade
```

## 9. Add Route Certificate

Edit the route and add the certificate (only needed when deploying the app for the first time).

## Done!

Your pygeoapi server and postgis database should now be set up and running. Go to the URL you gave and enjoy.

## Print Request Log

```
flask --app src.app print_log <limit>
```

## Deleting All Resources

If you need to remove the dev (or virva-dev) version to save resources, you can delete all resources that are associated
with that version with this command

```
oc delete all,configmap,secret,pvc,serviceaccount,role,rolebinding --selector version=dev
```

Don't use this command with prod version as it deletes all the data.
