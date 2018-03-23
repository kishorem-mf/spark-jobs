package com.unilever.ohub.spark.tsv2parquet

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql._

import scala.reflect.runtime.universe._

abstract class DomainGateKeeper[ToType <: Product : TypeTag] extends SparkJob {
  protected def fieldSeparator: String
  protected def hasHeaders: Boolean
  protected def partitionByValue: Seq[String]

  protected def transform: Row => ToType

  override final val neededFilePaths = Array("INPUT_FILE", "OUTPUT_FILE")

  implicit def encodeToObject: Encoder[ToType] = Encoders.product[ToType]

  override def run(spark: SparkSession, filePaths: Product, storage: Storage): Unit = {
    val (inputFile: String, outputFile: String) = filePaths

    val result = storage
      .readFromCsv(
        location = inputFile,
        fieldSeparator = fieldSeparator,
        hasHeaders = hasHeaders
      )
      .map(transform)

    storage.writeToParquet(result, outputFile, partitionByValue)
  }
}
