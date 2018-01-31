package com.unilever.ohub.spark.generic

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession

object FileSystems extends App{
  def removeFullDirectoryUsingHadoopFileSystem(spark:SparkSession, filePath:String):Boolean = {
    import org.apache.hadoop.fs.{Path => hadoopPath}
    try{
      val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      fileSystem.delete(new hadoopPath(filePath), true)
    } catch {
      case _: Exception => false
    }
    true
  }
}
