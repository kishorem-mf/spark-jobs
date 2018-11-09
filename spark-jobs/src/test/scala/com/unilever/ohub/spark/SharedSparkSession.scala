package com.unilever.ohub.spark

import java.io.File
import java.nio.file.{ Path, Paths }

import org.apache.spark.sql.SparkSession

/**
 * SharedSparkSession is a spark session usable for unit tests that share a single spark object. By
 * using the same object only one spark session is created for all tests.
 */
object SharedSparkSession {
  lazy val spark: SparkSession = createSparkSession

  private val resourcesPath: Path =
    Paths.get(new java.io.File(".").getAbsolutePath, "src", "test", "resources")
  private val warehousePath: String =
    new File(resourcesPath.toFile, "spark-warehouse").getAbsolutePath
  private val checkpointPath: String =
    new File(resourcesPath.toFile, "checkpoints").getAbsolutePath

  private def createSparkSession: SparkSession = {
    val session = SparkSession
      .builder()
      .appName("spark_test")
      .master("local[4]")
      .config("spark.sql.warehouse.dir", warehousePath)
      .config("spark.ui.enabled", "false")
      .config("spark.driver.memory", "4g")
      .getOrCreate()
    session.sparkContext.setCheckpointDir(checkpointPath)
    session
  }
}