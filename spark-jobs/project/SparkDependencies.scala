import sbt._
import sbt.Keys._
import LibraryVersions._

object SparkDependencies extends AutoPlugin {
  override val trigger = noTrigger
  override val requires = plugins.JvmPlugin

  private val sparkDependencyType: String = sys.props.getOrElse("sparkDependencyType", "compile")

  lazy val sparkDependencies: Seq[ModuleID] = Seq(
    "org.apache.spark"        %% "spark-core"          % SparkVersion  % sparkDependencyType excludeAll ExclusionRule(organization = "org.scalatest"),
    "org.apache.spark"        %% "spark-sql"           % SparkVersion  % sparkDependencyType,
    "org.apache.spark"        %% "spark-mllib"         % SparkVersion  % sparkDependencyType
  )

  object autoImport {
    lazy val depType = taskKey[Unit]("print sparkDependencyType")
  }

  import autoImport._

  override val projectSettings = Seq(
    depType := {
      println(s"$sparkDependencyType")
    },
    libraryDependencies ++= sparkDependencies
  )
}
