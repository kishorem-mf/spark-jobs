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

## Getting Dataproc logs

Airflow logs the state of its operators but unfortunately since we run the DAGs in an external cluster the actual spark logs are not available in airflow. To get to the logs:

- Open the logs of a task.
- You might see a line in there looking like
```
If logs are available, they can be found in 'gs://dataproc-7c851ecb-2106-4de0-9154-448af1b47461-europe-west4/google-cloud-dataproc-metainfo/a2eb824d-88fa-425d-bcd0-877aacfcad44/jobs/operators_to_parquet_20180307_2decfbfa/driveroutput'.
```
- The easiers way to view the logs is to use `gsutil`: https://cloud.google.com/storage/docs/gsutil_install
- Use gsutil to find the actual files in the folder with
`gsutil ls gs://dataproc-7c851ecb-2106-4de0-9154-448af1b47461-europe-west4/google-cloud-dataproc-metainfo/a2eb824d-88fa-425d-bcd0-877aacfcad44/jobs/operators_to_parquet_20180307_2decfbfa/` <-- note the omission of `driveroutput`. This will list the logs available, looks something like this:
```
gs://dataproc-7c851ecb-2106-4de0-9154-448af1b47461-europe-west4/google-cloud-dataproc-metainfo/a2eb824d-88fa-425d-bcd0-877aacfcad44/jobs/operators_to_parquet_20180307_2decfbfa/driveroutput.000000000
gs://dataproc-7c851ecb-2106-4de0-9154-448af1b47461-europe-west4/google-cloud-dataproc-metainfo/a2eb824d-88fa-425d-bcd0-877aacfcad44/jobs/operators_to_parquet_20180307_2decfbfa/driveroutput.000000001
```
- Depending on the number of retries set in you might see more or less files. The last number corresponds to the retry in airflow. To view the logs you can `cat` it (and pass it to less if to big) with: `gsutil cat gs://dataproc-7c851ecb-2106-4de0-9154-448af1b47461-europe-west4/google-cloud-dataproc-metainfo/a2eb824d-88fa-425d-bcd0-877aacfcad44/jobs/operators_to_parquet_20180307_2decfbfa/driveroutput.000000000 | less` 

Happy hunting!
