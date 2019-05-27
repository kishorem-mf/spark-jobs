# Integration-tests

## Building the dependencies
### TestImage
```docker build -t sparktest .```

First it copies the _pipelines_, _test data_ and _test scripts_ to the designated directories.
Then it launches a Spark cluster with two worker nodes. 

The base image is a custom scalaspark image. This can be install in the local registry by building the dockerfile located in the `engine-build-tools`-repo.

### Artifacts

Build the spark-jobs JAR by running `sbt assemble -DsparkDependencyType=provided`.

Build the python egg by: Running in linux/mac-os(because of the C++ executable architecture --> WSL(Windows Subsystem for Linux) can be used for this) and using python3(executable for python3 is asserted to be python on path, not python3) and call `engine\name-matching\compile_library.sh`.

## Running the test
For a decent shell/bash:
``` sh
docker run \
    -v <host-path-to-artefacts>/<path-to-spark-jobs>/:/usr/local/artefacts/sparkjobs \
    -v <host-path-to-artefacts>/<path-to-name-matching-egg>/:/usr/local/artefacts/name-matching/egg/ \
    -v <host-path-to-artefacts>/<pat-to-name-matching-main>/:/usr/local/artefacts/name-matching/main/ \
    sparktest bash -c "/usr/local/pipelines/all_pipelines.sh;pytest /usr/local/it-test/tests/"
```

docker run -it \
    -v /Users/timvancann/repos/unilever/engine/spark-jobs/target/scala-2.11:/usr/local/artefacts/sparkjobs \
    -v /Users/timvancann/repos/unilever/engine/name-matching/dist:/usr/local/artefacts/name-matching/egg/ \
    -v /Users/timvancann/repos/unilever/engine/name-matching/string_matching_package:/usr/local/artefacts/name-matching/main/ \
    -v /Users/timvancann/repos/unilever/engine/sparktest/pipelines:/usr/local/pipelines \
    -v /Users/timvancann/repos/unilever/engine/sparktest/it-test:/usr/local/it-test \
    sparktest bash
```

For windows:
````PowerShell
docker run `
    -v <host-path-to-artefacts>/<path-to-spark-jobs>/:/usr/local/artefacts/sparkjobs `
    -v <host-path-to-artefacts>/<path-to-name-matching-egg>/:/usr/local/artefacts/name-matching/egg/ `
    -v <host-path-to-artefacts>/<pat-to-name-matching-main>/:/usr/local/artefacts/name-matching/main/ `
    sparktest bash -c "/usr/local/pipelines/all_pipelines.sh;pytest /usr/local/it-test/tests/"
```

Then it will run all the data pipelines and finally it runs the integration test suite to verify the results.
