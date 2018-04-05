name := "spark-jobs"

version := "0.2.0"

scalaVersion := "2.11.11"

val sparkVersion = "2.2.1"
val sparkDependencyType = sys.props.getOrElse("sparkDependencyType", "compile")
lazy val depType = taskKey[Unit]("print sparkDependencyType")

depType := {
  println(s"$sparkDependencyType")
}

Defaults.itSettings
Test / testOptions += Tests.Argument("-oD")
IntegrationTest / testOptions += Tests.Argument("-oD")

libraryDependencies ++= Seq(
  "org.apache.spark"    %% "spark-core"     % sparkVersion  % sparkDependencyType excludeAll ExclusionRule(organization = "org.scalatest"),
  "org.apache.spark"    %% "spark-sql"      % sparkVersion  % sparkDependencyType,
  "org.apache.spark"    %% "spark-mllib"    % sparkVersion  % sparkDependencyType,
  "org.postgresql"      %  "postgresql"     % "42.1.4",
  "org.apache.commons"  %  "commons-lang3"  % "3.6",
  "org.scalatest"       %% "scalatest"      % "3.0.4"       % "test,it",
  "org.scalamock"       %% "scalamock"      % "4.0.0"       % "test,it"
)

lazy val root = project.in(file(".")).configs(IntegrationTest)

test in assembly := {}

// Scoverage settings
coverageExcludedPackages := "<empty>;.*storage.*"
coverageMinimum := 29.9
coverageFailOnMinimum := true
