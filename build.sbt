name := "spark-jobs"

version := "0.1"

scalaVersion := "2.11.11"

val sparkVersion = "2.1.0" // 2.1.0 is the latest version supported by Azure as of 22/11/17

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "compile"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "compile"
