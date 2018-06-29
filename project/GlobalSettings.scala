import sbt.{Def, _}
import sbt.Keys._
import LibraryVersions._
import com.typesafe.sbt.SbtScalariform.ScalariformKeys._
import scalariform.formatter.preferences._
import sbtassembly.AssemblyKeys.assembly
import scoverage.ScoverageKeys._

object ProjectAutoPlugin extends AutoPlugin {
  override val trigger = noTrigger
  override val requires = plugins.JvmPlugin

  override val projectSettings = Seq(
    scalaVersion := "2.11.12",
    libraryDependencies ++= projectDependencies
  )

  val testSettings: Seq[_] = {
    val flags = Seq(Tests.Argument("-oD"))
    //    Defaults.itSettings
    //    IntegrationTest / testOptions ++= flags
    Seq(
      test in assembly := {},
      parallelExecution in Test := false,
      Test / testOptions ++= flags
    )
  }

  val formattingSettings = Seq(
    scalariformPreferences.value
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

  private val scoverageSettings: Seq[_] = Seq(
    // Scoverage settings
    coverageExcludedPackages := "<empty>",
    coverageMinimum := 70.0,
    coverageFailOnMinimum := true
  )

  private val projectDependencies = Seq(
    "org.postgresql" % "postgresql" % "42.2.2",
    "org.apache.commons" % "commons-lang3" % "3.7",
    "io.circe" %% "circe-core" % CirceVersion,
    "io.circe" %% "circe-generic" % CirceVersion,
    "io.circe" %% "circe-parser" % CirceVersion,
    "com.github.scopt" %% "scopt" % "3.7.0",
    "org.scalatest" %% "scalatest" % "3.0.5" % "test,it",
    "org.scalamock" %% "scalamock" % "4.1.0" % "test,it"
  ) ++ testSettings ++ scoverageSettings
}

