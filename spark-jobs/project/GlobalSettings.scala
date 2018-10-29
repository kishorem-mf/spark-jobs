import LibraryVersions._
import com.typesafe.sbt.SbtScalariform
import sbt.Keys._
import sbt._
import sbtassembly.AssemblyKeys.assembly
import scalariform.formatter.preferences._
import scoverage.ScoverageKeys._

object GlobalSettings extends AutoPlugin {
  override val trigger = allRequirements
  override val requires = plugins.JvmPlugin

  override val projectSettings: Seq[Setting[_]] = Seq(
    scalaVersion := "2.11.12",
    libraryDependencies ++= projectDependencies
  ) ++ testSettings ++ scoverageSettings ++ scalariFormSettings ++ forceDepsSettings

  lazy val testSettings: Seq[Setting[_]] = {
    val flags = Seq(Tests.Argument("-oD"))
    //    Defaults.itSettings
    //    IntegrationTest / testOptions ++= flags
    Seq(
      test in assembly := {},
      parallelExecution in Test := false,
      Test / testOptions ++= flags
    )
  }

  lazy val scalariFormSettings: Seq[Setting[_]] = Seq(
    SbtScalariform.autoImport.scalariformPreferences :=
      SbtScalariform.autoImport.scalariformPreferences.value
        .setPreference(AlignParameters, false)
        .setPreference(AlignSingleLineCaseStatements, true)
        .setPreference(AlignSingleLineCaseStatements.MaxArrowIndent, 90)
        .setPreference(DoubleIndentConstructorArguments, true)
        .setPreference(RewriteArrowSymbols, true)
        .setPreference(DanglingCloseParenthesis, Preserve)
        .setPreference(IndentSpaces, 2)
        .setPreference(IndentWithTabs, false)
        .setPreference(NewlineAtEndOfFile, true)
  )

  lazy val scoverageSettings: Seq[Setting[_]] = Seq(
    // Scoverage settings
    coverageExcludedPackages := "<empty>",
    coverageMinimum := 60.0,
    coverageFailOnMinimum := true
  )

  lazy val projectDependencies: Seq[ModuleID] = Seq(
    "org.apache.commons" % "commons-lang3" % "3.8.1",
    "org.postgresql" % "postgresql" % "42.2.5",
    "io.circe" %% "circe-core" % CirceVersion,
    "io.circe" %% "circe-generic" % CirceVersion,
    "io.circe" %% "circe-parser" % CirceVersion,
    "com.github.scopt" %% "scopt" % "3.7.0",
    "org.scalatest" %% "scalatest" % "3.0.5" % "test,it",
    "org.scalamock" %% "scalamock" % "4.1.0" % "test,it",
    "com.h2database" % "h2" % "1.4.197" % "test,it"
  )

  /**
    * Fixes version conflicts warnings
    */
  lazy val forceDepsSettings: Seq[Setting[_]] = Seq(
    dependencyOverrides ++= Seq(
      "com.google.code.findbugs" % "jsr305" % "3.0.2",
      "io.netty" % "netty" % "3.9.9.Final",
      "commons-net" % "commons-net" % "2.2",
      "com.google.guava" % "guava" % "11.0.2"
    )
  )
}

