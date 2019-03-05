
lazy val `spark-jobs` = project.in(file("."))
  .enablePlugins(SparkDependencies, GlobalSettings)
  .configs(IntegrationTest)

assembly / test := {}

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
