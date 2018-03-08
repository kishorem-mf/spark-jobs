# Airflow DAGs

This repository holds the airflow dags for OHUB2.0 and O-Universe.

To start writing a DAG for airflow have a look at the existing DAGs or the [airflow tutorial](https://airflow.apache.org/tutorial.html).

## Deployment


## Viewing the UI
The UI has been shielded behind local port forwarding. To access the UI:

- Install the gcloud cli tools: https://cloud.google.com/sdk/downloads
- Login with your user using `gcloud auth` (if you have not been added to the project, ask @Constantijn): https://cloud.google.com/sdk/gcloud/reference/auth/
- Install kubectl cli: https://kubernetes.io/docs/tasks/tools/install-kubectl/#download-as-part-of-the-google-cloud-sdk
- Get the credentials for the Kubernetes cluster: https://cloud.google.com/sdk/gcloud/reference/container/clusters/get-credentials, using: `gcloud container clusters get-credentials cluster-1 --zone europe-west4-c`

All these steps have to be done (unless you logout) only once. Next:

- Find the kubernetes podname for airflow with: `kubectl get pod`. Should look something like:
```
NAME                                 READY     STATUS    RESTARTS   AGE
airflow-webserver-799c7bd695-mbgk8   3/3       Running   0          5d
```
- Forward the airflow port to your localhost with `kubectl port-forward [podname] [localport]:8080`
In this example `[podname]` is `airflow-webserver-799c7bd695-mbgk8`
- Acces the UI in the browser at `http://localhost:[localport]`