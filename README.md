# Airflow DAGs

This repository holds the airflow dags for OHUB2.0 and O-Universe.

To start writing a DAG for airflow have a look at the existing DAGs or the [airflow tutorial](https://airflow.apache.org/tutorial.html).

## Deployment


## Viewing the UI
The UI has been shielded behind local port forwarding. To access the UI:

- Install the az cli tools: https://docs.microsoft.com/en-us/cli/azure/install-azure-cli?view=azure-cli-latest
- Login with your user using `az login` 
- Install kubectl cli: https://kubernetes.io/docs/tasks/tools/install-kubectl/
- [OPTIONAL] Install kubectl completion (highly recommended!) https://kubernetes.io/docs/tasks/tools/install-kubectl/#enabling-shell-autocompletion
- Get the credentials for the Kubernetes cluster: 
```
az aks get-credentials -g bieno-da-s-60072-containers-rg -n ul-ohub2-aks-dev-we
```

All these steps have to be done (unless you logout) only once. Next:

- Find the kubernetes podname for airflow with: `kubectl get pod`. Should look something like:
```
NAME                                 READY     STATUS    RESTARTS   AGE
airflow-webserver-799c7bd695-mbgk8   3/3       Running   0          5d
```
- Forward the airflow port to your localhost with 

```
kubectl port-forward [podname] [localport]:8080
```

In this example `[podname]` is `airflow-webserver-799c7bd695-mbgk8`
- Acces the UI in the browser at [http://localhost:[localport]](http://localhost:[localport])
