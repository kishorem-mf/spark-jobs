name := "spark-jobs"

version := "0.1"

scalaVersion := "2.11.11"

val sparkVersion = "2.2.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "compile"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "compile"
