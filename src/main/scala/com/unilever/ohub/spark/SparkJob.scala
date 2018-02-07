package com.unilever.ohub.spark

import com.unilever.ohub.spark.storage.{ DiskStorage, Storage }
import org.apache.log4j.{ LogManager, Logger }
import org.apache.spark.sql.SparkSession

trait SparkJob { self =>
  def neededFilePaths: Array[String]

  def run(spark: SparkSession, filePaths: Product, storage: Storage): Unit

  implicit protected val log: Logger = LogManager.getLogger(self.getClass)

  private def getFilePaths(args: Array[String]): Product = {
    if (args.length != neededFilePaths.length) {
      log.error("specify " + neededFilePaths.mkString("[", "], [", "]"))
      sys.exit(1)
    }

    args.length match {
      case 2 => (args(0), args(1))
      case 3 => (args(0), args(1), args(2))
    }
  }

  def main(args: Array[String]): Unit = {
    val jobName = self.getClass.getSimpleName

    val filePaths = getFilePaths(args)

    val spark = SparkSession
      .builder()
      .appName(jobName)
      .getOrCreate()

    val storage = new DiskStorage(spark)

    val startOfJob = System.currentTimeMillis()

    run(spark, filePaths, storage)

    log.info(s"Done in ${(System.currentTimeMillis - startOfJob) / 1000}s")
  }
}
