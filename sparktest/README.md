You can built the docker image with

```docker build -t sparktest .```

First it copies the _pipelines_, _test data_ and _test scripts_ to the designated directories.
Then it launches a Spark cluster with two worker nodes. 

Next you can run the docker image with

```docker run 
    -v <host-path-to-artefacts>/<path-to-spark-jobs>/:/usr/local/artefacts/sparkjobs 
    -v <host-path-to-artefacts>/<path-to-name-matching-egg>/:/usr/local/artefacts/name-matching/egg/ 
    -v <host-path-to-artefacts>/<pat-to-name-matching-main>/:/usr/local/artefacts/name-matching/main/ 
    sparktest bash -c "/usr/local/pipelines/all_pipelines.sh;pytest /usr/local/it-test/tests/"
```

Then it will run all the data pipelines and finally it runs the integration test suite to verify the results.
