name := "spark-jobs"

version := "0.1"

scalaVersion := "2.11.11"

val sparkVersion = "2.2.0" // 2.1.0 is the latest version supported by Azure as of 22/11/17
val sparkDependencyType = sys.props.getOrElse("sparkDependencyType", "compile")
lazy val depType = taskKey[Unit]("print sparkDependencyType")

depType := {
  println(s"$sparkDependencyType")
}

Defaults.itSettings
testOptions in Test += Tests.Argument("-oD")
testOptions in IntegrationTest += Tests.Argument("-oD")

libraryDependencies ++= Seq(
  "org.apache.spark"    %% "spark-core"     % sparkVersion  % sparkDependencyType excludeAll ExclusionRule(organization = "org.scalatest"),
  "org.apache.spark"    %% "spark-sql"      % sparkVersion  % sparkDependencyType,
  "org.apache.spark"    %% "spark-mllib"    % sparkVersion  % sparkDependencyType,
  "org.postgresql"      %  "postgresql"     % "42.1.4",
  "org.apache.commons"  %  "commons-lang3"  % "3.6",
  "org.scalatest"       %% "scalatest"      % "3.0.4"       % "test,it"
)

lazy val root = project.in(file(".")).configs(IntegrationTest)
