# Airflow DAGs

This repository holds the Airflow dags for OHUB2.0 and O-Universe.

To start writing a DAG for Airflow have a look at the existing DAGs or the [Airflow tutorial](https://airflow.apache.org/tutorial.html).

## Deployment
Every merge to `master` automatically deploys all dags to a file share in Azure (storage account: `ulohub2sadevne`, fileshare: `airflow-dags`. This fileshare is mounted as volume in the Airflow kubernetes deployment. Every change on this fileshare is therefore directly picked up by Airflow. Thus the DAGs are refreshed.

Note: Files deleted in Git are NOT deleted on the fileshare. If DAGs are removed from the master branch they have to be manually removed from the fileshare. Also: removed DAGs will still show up in the Airflow UI since they are not removed from the Airflow metadata database. You have to manually do this if you want to.


## Viewing the UI
The UI has been shielded behind local port forwarding. To access the UI:

- Install the [`az` cli tools](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli?view=azure-cli-latest)
- Login with your user using `az login` 
- Install [`kubectl` cli](https://kubernetes.io/docs/tasks/tools/install-kubectl/)
- [OPTIONAL] Install [kubectl completion](https://kubernetes.io/docs/tasks/tools/install-kubectl/#enabling-shell-autocompletion) (highly recommended!)
- Get the credentials for the Kubernetes cluster: 
```
az aks get-credentials -g bieno-da-s-60072-containers-rg -n ul-ohub2-aks-dev-we
```

All these steps have to be done (unless you logout) only once. Next:

- Find the kubernetes podname for Airflow with: `kubectl get pod`. Should look something like:
```
NAME                                 READY     STATUS    RESTARTS   AGE
airflow-webserver-799c7bd695-mbgk8   3/3       Running   0          5d
```
- Forward the Airflow port to your localhost with 

```
kubectl port-forward [podname] [localport]:8080
```

In this example `[podname]` is `airflow-webserver-799c7bd695-mbgk8`
- Access the UI in the browser at [http://localhost:[localport]](http://localhost:[localport])
