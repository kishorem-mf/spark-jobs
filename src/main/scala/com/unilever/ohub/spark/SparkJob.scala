package com.unilever.ohub.spark

import com.unilever.ohub.spark.storage.{ DefaultStorage, Storage }
import org.apache.log4j.{ LogManager, Logger }
import org.apache.spark.sql.SparkSession

trait SparkJob { self ⇒
  def neededFilePaths: Array[String]

  def optionalFilePaths: Array[String] = Array.empty

  def run(spark: SparkSession, filePaths: Product, storage: Storage): Unit

  implicit protected val log: Logger = LogManager.getLogger(self.getClass)

  private def getFilePaths(args: Array[String]): Product = {
    val argRange = Range.inclusive(neededFilePaths.length, neededFilePaths.length + optionalFilePaths.length)

    if (!argRange.contains(args.length)) {
      log.error(s"specify mandatory '${neededFilePaths.mkString("[", "], [", "]")}' and optional '${optionalFilePaths.mkString("[", "], [", "]")}'")
      sys.exit(1)
    }

    args.length match {
      case 2 ⇒ (args(0), args(1))
      case 3 ⇒ (args(0), args(1), args(2))
      case 4 ⇒ (args(0), args(1), args(2), args(3))
      case 5 ⇒ (args(0), args(1), args(2), args(3), args(4))
      case 6 ⇒ (args(0), args(1), args(2), args(3), args(4), args(5))
      case 7 ⇒ (args(0), args(1), args(2), args(3), args(4), args(5), args(6))
      case 8 ⇒ (args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7))
    }
  }

  def main(args: Array[String]): Unit = {
    val jobName = self.getClass.getSimpleName

    val filePaths = getFilePaths(args)

    val spark = SparkSession
      .builder()
      .appName(jobName)
      .getOrCreate()

    val storage = new DefaultStorage(spark)

    val startOfJob = System.currentTimeMillis()

    run(spark, filePaths, storage)

    log.info(s"Done in ${(System.currentTimeMillis - startOfJob) / 1000}s")
  }
}
