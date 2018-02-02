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

  def getFileNames(args: Array[String])(implicit log: Logger): (String, String, String) = {
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
}
