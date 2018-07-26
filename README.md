# Airflow DAGs

This repository holds the Airflow dags for OHUB2.0 and O-Universe.

To start writing a DAG for Airflow have a look at the existing DAGs or the [Airflow tutorial](https://airflow.apache.org/tutorial.html).

## Development
Put your DAGS into the `dags` folder, The `dags/custom_operators` is reserved for implementation of operators not present in `apache-airflow`. Use the word `dag` in the python files that are actual DAGs to separate configuration settings from DAGs. Placing DAGs in subfolders to group them is fine and will be picked up by airflow, however they are not grouped in the UI. To make sure they are grouped in the UI, come up with a prefix for the group and use that in the DAG `id`.

### Running tests
To run tests and  codestyle. Create and activate the python environment:

- Download and install [miniconda](https://conda.io/miniconda.html)
- `cd` into this repo in a terminal
- Create and activate the `conda` environment with
```
conda env create -f environment.yml
source activate airflow-dags
```
Now you can run the tests with:
```
python -m pytest --cov-config .coveragerc --cov=./dags tests
```
And check the codestyle with:
```
pycodestyle --show-source .
```

### Local UI
Requirements: 

- Ensure that you have logged in to the docker azure registry with `docker login`. You can find the credentials in the Azure Portal -> shared resource group -> container registry -> access keys
- Ensure that you have [docker](https://docs.docker.com/install/) installed and running

Run the `local_dev.sh` script. The airflow UI should now be available at `http://localhost:8070`

To check if DAGs can be loaded properly, you can use (this is executed automatically in the aforementioned script)
```
docker exec -i ulohub_airflow_dags airflow list_dags
```
Any errors in any DAG setup will be reported using that command.

## Deployment
Every merge to `master` automatically deploys all dags to a file share in Azure (storage account: `ulohub2sadevne`, fileshare: `airflow-dags`. This fileshare is mounted as volume in the Airflow kubernetes deployment. Every change on this fileshare is therefore directly picked up by AirFlow. Thus the DAGs are refreshed.

Note: Files deleted in Git are NOT deleted on the fileshare. If DAGs are removed from the master branch they have to be manually removed from the fileshare. Also: removed DAGs will still show up in the Airflow UI since they are not removed from the AirFlow metadata database. You have to manually do this if you want to. For this, there is a handy script called `remove_dag.sh`. It requires the credentials to the Postgres instance on Azure holding the airflow metadata and a dag id.


## Viewing the remote UI on Azure
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

Or, in one go:
```
kubectl port-forward `kubectl get pods | grep airflow-webserver| awk '{print $1}'` 8080:8080
```

In this example `[podname]` is `airflow-webserver-799c7bd695-mbgk8`
- Access the UI in the browser at [http://localhost:[localport]](http://localhost:[localport])
