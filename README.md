# AirFlow DAGs

This repository holds the AirFlow dags for OHUB2.0 and O-Universe.

To start writing a DAG for AirFlow have a look at the existing DAGs or the [AirFlow tutorial](https://airflow.apache.org/tutorial.html).

## Development
Put your DAGS into the `dags` folder, The `dags/custom_operators` is reserved for implementation of operators not present in apache-airflow. Use the word `dag` in the python files that are actual DAGs to separate configuration settings from DAGs. Placing DAGs in subfolders to group them is fine and will be picked up by airflow, however they are not grouped in the UI. To make sure they are grouped in the UI, come up with a prefix for the group and use that in the DAG `id`.

To run tests, codestyle and see the dags in a local UI. Create and activate the python environment:

- Download and install [miniconda](https://conda.io/miniconda.html)
- `cd` into this repo in a terminal
- Create and activate the `conda` environment with
```
conda env create -f environment.yml
source activate airflow-dags
```
- Run the `local_dev.sh` script to further setup local development. This will 
  
  - Install any remaining dependencies
- Set some local ENV variables such that airflow does not need a config:
```
export AIRFLOW_HOME="`pwd`"
export AIRFLOW__CORE__LOAD_EXAMPLES="False"
export AIRFLOW__CORE__DAGS_FOLDER="$AIRFLOW_HOME/dags"
export AIRFLOW__CORE__EXECUTOR="SequentialExecutor"
export AIRFLOW__CORE__BASE_LOG_FOLDER="$AIRFLOW_HOME/logs"
```
- Fire up the airflow webserver with:
```
airflow webserver
```


## Deployment
Every merge to `master` automatically deploys all dags to a file share in Azure (storage account: `ulohub2sadevne`, fileshare: `airflow-dags`. This fileshare is mounted as volume in the AirFlow kubernetes deployment. Every change on this fileshare is therefore directly picked up by AirFlow. Thus the DAGs are refreshed.

Note: Files deleted in Git are NOT deleted on the fileshare. If DAGs are removed from the master branch they have to be manually removed from the fileshare. Also: removed DAGs will still show up in the AirFlow UI since they are not removed from the AirFlow metadata database. You have to manually do this if you want to.


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

- Find the kubernetes podname for AirFlow with: `kubectl get pod`. Should look something like:
```
NAME                                 READY     STATUS    RESTARTS   AGE
airflow-webserver-799c7bd695-mbgk8   3/3       Running   0          5d
```
- Forward the AirFlow port to your localhost with 

```
kubectl port-forward [podname] [localport]:8080
```

In this example `[podname]` is `airflow-webserver-799c7bd695-mbgk8`
- Access the UI in the browser at [http://localhost:[localport]](http://localhost:[localport])
