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

# Build and Test
run `sbt package`.  
(We might need to switch to sbt-assembly later on)

# Tips
run `sbt dependencyUpdates` to check for updates on the dependencies.
