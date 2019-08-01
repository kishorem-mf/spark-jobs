
lazy val `spark-jobs` = project.in(file("."))
  .enablePlugins(SparkDependencies, GlobalSettings)
  .configs(IntegrationTest)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assembly / assemblyShadeRules := Seq(
  ShadeRule.rename("com.google.common.**" -> "repackaged.com.google.common.@1").inAll
)