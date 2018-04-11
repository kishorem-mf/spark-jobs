package com.unilever.ohub.spark.parquet

import org.apache.spark.sql.SparkSession
import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.functions._

object FuzzitSaleUpserter extends SparkJob {
  override final val neededFilePaths = Array("INPUT_FILE", "OUTPUT_FILE")

  override def run(spark: SparkSession, filePaths: Product, storage: Storage): Unit = {
    import spark.implicits._
    val (inputFile: String, outputFile: String) = filePaths
    val df = spark.read.parquet(inputFile)
    val df_ = df.groupBy($"salesLineUid").agg(max(df.col("date")))
    storage.writeToParquet(df_, outputFile)
  }
}
