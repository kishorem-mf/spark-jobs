package com.unilever.ohub.spark.generic

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{ Path => hadoopPath }

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
}
