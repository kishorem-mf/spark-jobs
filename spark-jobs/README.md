# Introduction 
Spark jobs for the OHub2 project.

# Getting Started
- install scala and sbt
- git clone this project
- run sbt compile
- install an editorconfig plugin for your editor (http://editorconfig.org/#download)
- open the project in your editor (like IntelliJ)
- pick a converter to run and run it from your editor
- Be sure to pass in the VM option `-Dspark.master=local[*]` when running the jobs on your local machine. (You'll get an error about missing a spark master if you don't)
- You can access the spark web UI to monitor the execution behavior at http://localhost:4040, see also https://spark.apache.org/docs/latest/monitoring.html for more information.

# Build and Test
run `sbt package`.
## If sbt task are available as run configurations (as in Intellij):
create a sbt task with `clean assembly` as tasks and add
`-Xms512M -Xmx1024M -Xss1M -XX:+CMSClassUnloadingEnabled -DsparkDependencyType=provided` as VM params

# Tips
run `sbt dependencyUpdates` to check for updates on the dependencies.
run `sbt clean coverage test coverageReport` to run the tests with coverage.
## If sbt task are available as run configurations (as in Intellij):
create a sbt task with `"set test in assembly := {}" clean assembly` as tasks (with same VM params as assembly)
to build jar while skipping the tests
