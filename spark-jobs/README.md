# Introduction 
Spark jobs for the OHub2 project.

# Getting Started

- install Java 8 
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

# Windows setup

If you don't have Hadoop installed locally it will complain about: `Missing winutils.exe (Failed to locate the winutils binary in the hadoop binary path) `
To solve this:

- Download `winutils.exe` from https://github.com/steveloughran/winutils/blob/master/hadoop-2.7.1/bin/winutils.exe
- Create a new directory for example: `c:\hadoop\bin` and place `winutils.exe` in this directory
- Add `c:\hadoop\bin` to your path or specify `-Dhadoop.home.dir=C:\hadoop` to your run configuration


