package com.unilever.ohub.spark.acm

import com.unilever.ohub.spark.generic.{ FileSystems, SparkFunctions }
import org.apache.log4j.Logger
import org.apache.spark.sql.{ DataFrame, SaveMode, SparkSession }

trait AcmConverterHelpers {
  def writeDataFrameToCSV(df: DataFrame, outputFile: String): Unit = {
    df
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("encoding", "UTF-8")
      .option("header", "true")
      .option("delimiter","\u00B6")   /* If needed .option("quoteAll","true") can be used for putting values in quotes */
      .option("quote", "\u0000")      /* This makes sure the when " is in a value it is not escaped like \" which is not accepted by ACM */
      .csv(outputFile)
  }

  def finish(
    spark: SparkSession,
    outputFile: String,
    outputParquetFile: String,
    outputFileNewName: String
  )(implicit log: Logger): Unit = {
    FileSystems.removeFullDirectoryUsingHadoopFileSystem(spark, outputParquetFile) match {
      case Left(e) => log.error(s"Could not remove directory [$outputParquetFile]", e)
      case _ =>
    }
    SparkFunctions.renameSparkCsvFileUsingHadoopFileSystem(spark, outputFile, outputFileNewName) match {
      case Left(e) => log.error(s"Could not rename file [$outputFile]", e)
      case _ =>
    }
  }
}
