
lazy val `spark-jobs` = project.in(file("."))
  .enablePlugins(SparkDependencies)
  .configs(IntegrationTest)
