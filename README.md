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
- Find the kubernetes service port for airflow with: `kubectl get service`. Should look something like:
```NAME                        TYPE        CLUSTER-IP     EXTERNAL-IP   PORT(S)                       AGE
airflow-webserver-service   NodePort    10.43.251.37   none        8080:30588/TCP,25:30288/TCP   1h
kubernetes                  ClusterIP   10.43.240.1    none        443/TCP                       10d
```
Note the nodeport for the airflow UI. (i.e. `8080:30588/TCP` so the nodeport is `30588`.
- ssh into the cluster with `gcloud compute ssh gke-cluster-1-default-pool-b63e03a0-60p4 -- -L [your_local_port]:localhost:[nodeport]`
- Acces the UI in the browser at http://localhost:your_local_port

All these steps have to be done (unless you logout) only once. Simply ssh into the cluster and you’re done. If the port doesn’t work doublecheck if the nodeport is still valid with the command mentioned earlier. Happy hacking! (edited)

