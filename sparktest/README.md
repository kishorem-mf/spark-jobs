When you built this docker image with

```docker build -t sparktest .```

it first copies the _artifacts_, _test data_ and _test scripts_ to the designated directories.
Then it launches a Spark cluster with two worker nodes. Next it runs the data pipelines for _operators_ and _contactpersons_.
And finally it runs the integration test suite to verify the results.
