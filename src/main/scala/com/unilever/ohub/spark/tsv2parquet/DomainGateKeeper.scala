package com.unilever.ohub.spark.tsv2parquet

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql._

import scala.reflect.runtime.universe._

object DomainGateKeeper {
  type ErrorMessage = String
}

abstract class DomainGateKeeper[ToType <: Product : TypeTag] extends SparkJob {
  import DomainGateKeeper._

  protected def fieldSeparator: String
  protected def hasHeaders: Boolean
  protected def partitionByValue: Seq[String]

  protected def transform: Row => Either[(Row, ErrorMessage), ToType]

  override final val neededFilePaths = Array("INPUT_FILE", "OUTPUT_FILE")

  implicit def encodeToObject: Encoder[ToType] = Encoders.product[ToType]

  override def run(spark: SparkSession, filePaths: Product, storage: Storage): Unit = {
    val (inputFile: String, outputFile: String) = filePaths

    val result: Dataset[ToType] = storage
      .readFromCsv(
        location = inputFile,
        fieldSeparator = fieldSeparator,
        hasHeaders = hasHeaders
      )
      .map(transform)
      // we could do something else with the errors here
      .filter(_.isRight)
      .map {
        case Right(result) => result
      }

    // only the correct results are written to parquet

    storage.writeToParquet(result, outputFile, partitionByValue)
  }
}
