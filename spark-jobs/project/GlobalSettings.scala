import sbt.Keys._
import sbt._
import sbtassembly.AssemblyKeys.assembly
import scoverage.ScoverageKeys._

object GlobalSettings extends AutoPlugin {
  override val trigger = allRequirements
  override val requires = plugins.JvmPlugin

  override val projectSettings = Seq(
    scalaVersion := "2.11.12",
    libraryDependencies ++= projectDependencies
  ) ++ testSettings ++ scoverageSettings ++ forceDepsSettings

  lazy val testSettings = {
    val flags = Seq(Tests.Argument("-oD"))
    Seq(
      assembly / test := {},
      Test / parallelExecution := false,
      Test / testOptions ++= flags
    )
  }

  lazy val scoverageSettings = Seq(
    coverageExcludedPackages := "<empty>",
    coverageMinimum := 70.0,
    coverageFailOnMinimum := true
  )

  lazy val projectDependencies = Seq(
    "com.github.scopt" %% "scopt" % "3.7.1",
    "org.scalatest" %% "scalatest" % "3.0.8" % "test,it",
    "org.scalamock" %% "scalamock" % "4.4.0" % "test,it",
    "org.reflections" % "reflections" % "0.9.11"

  )

  /**
    * Fixes version conflicts warnings
    */
  lazy val forceDepsSettings: Seq[Setting[_]] = Seq(
    dependencyOverrides ++= Seq(
      "com.google.code.findbugs" % "jsr305" % "3.0.2",
      "io.netty" % "netty" % "3.9.9.Final",
      "commons-net" % "commons-net" % "2.2"
    )
  )
}

