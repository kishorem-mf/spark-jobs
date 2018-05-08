# Local development

Make sure you have installed docker

Pull and step into the docker container with

```bash
docker pull fokkodriesprong/docker-pyspark
docker run -it -v $(pwd):/tmp fokkodriesprong/docker-pyspark /bin/bash
```

Inside the container, `cd` into the working directory with and install the python environment
```bash
cd /tmp
./prepare_environment.sh
```
This will setup the right python environment and compiles and installs the cython module into the environment.

Now you can run all the tests with
```bash
cd string_matching_package
python -m pytest --cov-config .coveragerc --cov=./ tests
```
