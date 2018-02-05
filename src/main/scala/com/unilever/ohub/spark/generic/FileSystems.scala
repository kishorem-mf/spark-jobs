package com.unilever.ohub.spark.generic

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{ Path => hadoopPath }
import org.apache.log4j.Logger

object FileSystems {
  def removeFullDirectoryUsingHadoopFileSystem(
    spark: SparkSession,
    filePath: String
  ): Either[Exception, Boolean] = {
    try {
      val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val path = new hadoopPath(filePath)
      Right(fileSystem.delete(path, true))
    } catch {
      case e: Exception => Left(e)
    }
  }

  def getInputOutFileNames(args: Array[String])(implicit log: Logger): (String, String) = {
    if (args.length != 2) {
      log.error("specify INPUT_FILE OUTPUT_FILE")
      sys.exit(1)
    }

    args(0) -> args(1)
  }

  def getInputOutputOutputParquetFileNames(
    args: Array[String]
  )(implicit log: Logger): (String, String, String) = {
    val (inputFile, outputFile) = getInputOutFileNames(args)

    val outputParquetFile = {
      if (outputFile.endsWith(".csv")) outputFile.replace(".csv", ".parquet")
      else outputFile
    }

    (inputFile, outputFile, outputParquetFile)
  }

  def getInputMatchingInputOutputFileNames(
    args: Array[String]
  )(implicit log: Logger): (String, String, String) = {
    if (args.length != 3) {
      log.error("specify MATCHING_INPUT_FILE INPUT_FILE OUTPUT_FILE")
      sys.exit(1)
    }

    (args(0), args(1), args(2))
  }
}
