package com.unilever.ohub.spark.acm

import com.unilever.ohub.spark.generic.{ FileSystems, SparkFunctions }
import org.apache.log4j.Logger
import org.apache.spark.sql.{ DataFrame, SaveMode, SparkSession }

trait AcmConverterHelpers {
  protected def log: Logger

  def getFileNames(args: Array[String]): (String, String, String) = {
    if (args.length != 2) {
      log.error("specify INPUT_FILE OUTPUT_FILE")
      sys.exit(1)
    }

    val inputFile = args(0)
    val outputFile = args(1)
    val outputParquetFile = {
      if (outputFile.endsWith(".csv")) outputFile.replace(".csv", ".parquet")
      else outputFile
    }

    (inputFile, outputFile, outputParquetFile)
  }

  def writeDataFrameToCSV(df: DataFrame, outputFile: String): Unit = {
    df
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("encoding", "UTF-8")
      .option("header", "true")
      .option("delimiter", "\u003B")
      .option("quote", "\u0020")
      .csv(outputFile)
  }

  def finish(
    spark: SparkSession,
    outputFile: String,
    outputParquetFile: String,
    outputFileNewName: String
  ): Unit = {
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
