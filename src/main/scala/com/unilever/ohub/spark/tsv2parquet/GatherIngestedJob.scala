package com.unilever.ohub.spark.tsv2parquet

import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

case class GatherConfig(
    input: String = "path-to-input",
    output: String = "path-to-output"
) extends SparkJobConfig

object GatherJob extends SparkJob[GatherConfig] {

  override private[spark] def defaultConfig = GatherConfig()

  override private[spark] def configParser(): OptionParser[GatherConfig] =
    new scopt.OptionParser[GatherConfig]("Domain entity combiner") {
      head("gather ingested files", "1.0")
      opt[String]("input") required () action { (x, c) ⇒
        c.copy(input = x)
      } text "input is a string property"
      opt[String]("output") required () action { (x, c) ⇒
        c.copy(output = x)
      } text "output is a string property"
      version("1.0")
      help("help") text "help text"
    }

  override def run(spark: SparkSession, config: GatherConfig, storage: Storage): Unit = {
    val gathered = storage.readDataFrameFromParquet(config.input)
      .coalesce(10)

    storage.writeToParquet(gathered, config.output)
  }
}

