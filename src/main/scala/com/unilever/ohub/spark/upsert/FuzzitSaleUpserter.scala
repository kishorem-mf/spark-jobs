package com.unilever.ohub.spark.upsert

import org.apache.spark.sql.SparkSession
import com.unilever.ohub.spark.{ DefaultConfig, SparkJobWithDefaultConfig }
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.functions._

object FuzzitSaleUpserter extends SparkJobWithDefaultConfig {

  override def run(spark: SparkSession, config: DefaultConfig, storage: Storage): Unit = {
    import spark.implicits._

    val df = spark.read.parquet(config.inputFile)
    val df_ = df.groupBy($"salesLineUid").agg(max(df.col("date")))

    storage.writeToParquet(df_, config.outputFile)
  }
}
