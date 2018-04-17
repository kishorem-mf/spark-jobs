import scalariform.formatter.preferences._

name := "spark-jobs"

version := "0.2.0"

scalaVersion := "2.11.12"

val sparkVersion = "2.3.0"
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
  "org.apache.commons"  %  "commons-lang3"  % "3.7",
  "org.scalatest"       %% "scalatest"      % "3.0.5"       % "test,it",
  "org.scalamock"       %% "scalamock"      % "4.1.0"       % "test,it"
)

lazy val root = project.in(file(".")).configs(IntegrationTest)

test in assembly := {}

// Scoverage settings
coverageExcludedPackages := "<empty>;.*storage.*"
coverageMinimum := 44.1
coverageFailOnMinimum := true

scalariformPreferences := scalariformPreferences.value
  .setPreference(AlignParameters, false)
  .setPreference(AlignSingleLineCaseStatements, true)
  .setPreference(AlignSingleLineCaseStatements.MaxArrowIndent, 90)
  .setPreference(DoubleIndentConstructorArguments, true)
  .setPreference(RewriteArrowSymbols, true)
  .setPreference(DanglingCloseParenthesis, Preserve)
  .setPreference(IndentSpaces, 2)
  .setPreference(IndentWithTabs, false)
  .setPreference(NewlineAtEndOfFile, true)
