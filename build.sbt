name := "spark-jobs"

version := "0.1"

scalaVersion := "2.11.11"

val sparkVersion = "2.2.0" // 2.1.0 is the latest version supported by Azure as of 22/11/17
val sparkDependencyType = sys.props.getOrElse("sparkDependencyType", "compile")
lazy val depType = taskKey[Unit]("print sparkDependencyType")

depType := {
  println(s"$sparkDependencyType")
}
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % sparkDependencyType excludeAll ExclusionRule(organization = "org.scalatest")
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % sparkDependencyType
libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion % sparkDependencyType
libraryDependencies += "org.scalatest"    %% "scalatest" % "3.0.4" % Test
libraryDependencies += "org.postgresql"   %  "postgresql" % "42.1.4"
libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.6"
