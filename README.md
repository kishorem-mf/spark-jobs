# Airflow DAGs

This repository holds the Airflow dags for OHUB2.0 and O-Universe.

To start writing a DAG for Airflow have a look at the existing DAGs or the [Airflow tutorial](https://airflow.apache.org/tutorial.html).

## Development
Put your DAGS into the `dags` folder, Use the word `dag` in the python files that are actual DAGs to separate configuration settings from DAGs. Placing DAGs in subfolders to group them is fine and will be picked up by airflow, however they are not grouped in the UI. To make sure they are grouped in the UI, come up with a prefix for the group and use that in the DAG `id`.

The `ohub/operators` package is reserved for implementation of operators not present in `apache-airflow`. The `ohub/utils` package contains general utilities. Other subpackages can be created as needed.

NOTE: Use Python 3.6 (3.7 has an issue with Airflow https://issues.apache.org/jira/browse/AIRFLOW-2716)

## CI
With each push, a Bitbucket Pipeline is triggered.
There's a bit of overhead from spinning up CI containers.
It's faster to ensure all steps run successfully locally first, which you can do with:

```bash
make local-ci
```

The Bitbucket Pipeline calls tasks in the `Makefile`.
All tasks are called in the `local-ci` task.

## CD
TODO: describe how deployment is done.

## Adding python dependencies

1. update `./environment.yml`
2. run `conda env update -f environment.yml`
3. see if everything works as expected
4. switch to the [`docker-airflow`](https://bitbucket.org/UFS-nl/docker-airflow/) project
   a. apply the changes of step 1 to `environment.yml`
   b. commit and push
   c. wait for the BitBucket pipeline to complete
   d. switch back to the `airflow-dags` project
5. update line 1 of `docker/airflow/Dockerfile` to reference the build number of step 4c as the new base image version
6. complete whatever changes you need the new dependency for
7. commit and push

### Local UI

#### Running with Docker Compose

This repository includes a Docker Composition that lets you run Airflow locally, with the dags installed. You can bring
it up with this command:

```bash
docker-compose up --detach --build
```

First make sure you build the Docker container specified in the project `docker-airflow` this is used as base image in the `docker-compose.yml`

The Airflow webserver is exposed on [localhost:8080](http://localhost:8080/admin).

```bash
# Pull the logs of the Airflow container
docker-compose logs --follow webserver

# Get a shell in the Airflow container
docker-compose exec webserver bash
```

#### Running on your laptop

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

**Note:** Files deleted in Git are NOT deleted on the fileshare. If DAGs are removed from the master branch they have to be manually removed from the fileshare. Also: removed DAGs will still show up in the Airflow UI since they are not removed from the AirFlow metadata database. You have to manually do this if you want to. For this, there is a handy script called `remove_dag.sh`. It requires the credentials to the Postgres instance on Azure holding the airflow metadata and a dag id.

## Viewing the remote UI on Azure
The UI has been shielded behind local port forwarding. To access the UI:

- Install the [`az` cli tools](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli?view=azure-cli-latest)
- Login with your user using `az login` 
- Install [`kubectl` cli](https://kubernetes.io/docs/tasks/tools/install-kubectl/). Note: make sure you download 1.10.x version and not 1.11.0 which will be installed by default when using `brew install` there is a known issue with the 1.11 version, see [here](https://github.com/kubernetes/kubernetes/issues/65575)
- [OPTIONAL] Install [kubectl completion](https://kubernetes.io/docs/tasks/tools/install-kubectl/#enabling-shell-autocompletion) (highly recommended!)
- set your Azure CLI to use the right subscription
```
az account set --subscription bc41a9ef-b853-4107-aa12-cc8d592c8e91
```
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
