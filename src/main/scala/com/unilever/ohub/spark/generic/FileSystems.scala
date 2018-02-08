package com.unilever.ohub.spark.generic

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{ Path => hadoopPath }
import org.apache.log4j.Logger

import scala.util.Try

object FileSystems {
  def removeFullDirectoryUsingHadoopFileSystem(
    spark: SparkSession,
    filePath: String
  ): Try[Boolean] = {
    Try {
      val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val path = new hadoopPath(filePath)
      fileSystem.delete(path, true)
    }
  }

  def getFileNames(args: Array[String], fileTypes: String*)(implicit log: Logger): Product = {
    if (args.length != fileTypes.length) {
      log.error("specify " + fileTypes.mkString("[", "], [", "]"))
      sys.exit(1)
    }

    args.length match {
      case 2 => (args(0), args(1))
      case 3 => (args(0), args(1), args(2))
    }
  }
}
