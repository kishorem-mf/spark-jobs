package com.unilever.ohub.spark.generic

import java.util.Properties

import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.functions._
import StringFunctions._
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.{ Path => hadoopPath }

object SparkFunctions {
  def readJdbcTable(
    spark: SparkSession,
    dbConnectionString: String = "jdbc:postgresql://localhost:5432/",
    dbName: String = "ufs_example",
    dbTable: String,
    userName: String = "ufs_example",
    userPassword: String = "ufs_example"
  ): DataFrame = {
    val dbFullConnectionString = {
      if (dbConnectionString.endsWith("/")) s"$dbConnectionString$dbName"
      else s"$dbConnectionString/$dbName"
    }

    val jdbcProperties = new Properties
    jdbcProperties.put("user", userName)
    jdbcProperties.put("password", userPassword)

    spark.read.jdbc(dbFullConnectionString, dbTable, jdbcProperties)
  }

  def addIdFieldToDataFrameField(
    spark: SparkSession,
    dataFrame: DataFrame,
    fieldName: String
  ): DataFrame = {
    import spark.implicits._
    val dataFramePart = dataFrame.select(fieldName)
    val dataFrameFull = dataFramePart.withColumn("ID", lit(1))
    val dataSet = dataFramePart.as[String]
    val rdd = dataSet.rdd
    val rddWithId = rdd.zipWithUniqueId()
    rddWithId.toDF(dataFrameFull.columns: _*)
  }

  def getSetOfUniqueCharsInDataFrameColumn(dataFrame: DataFrame): Set[Char] = {
    def removeSquareBrackets(input: String): String = input.replace("[", "").replace("]", "")

    dataFrame
      .distinct()
      .collect()
      .map(_.toString())
      .map(removeSquareBrackets)
      .flatMap(_.toCharArray)
      .sortWith(_.compareTo(_) < 0)
      .toSet
  }

  def renameSparkCsvFileUsingHadoopFileSystem(
    spark: SparkSession,
    filePath: String,
    newFileName: String
  ): Either[Exception, Boolean] = {
    try {
      val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val originalPath = new hadoopPath(s"$filePath/part*")
      val file = fileSystem.globStatus(originalPath)(0).getPath
      Right {
        fileSystem.rename(
          new hadoopPath(s"${file.getParent}/${file.getName}"),
          new hadoopPath(s"${file.getParent.getParent}/${newFileName}_$getFileDateString.csv")
        )
        fileSystem.delete(
          new hadoopPath(s"${file.getParent}"),
          true
        )
        fileSystem.delete(
          new hadoopPath(s"${file.getParent.getParent}/.${newFileName}_$getFileDateString.csv.crc"),
          true
        )
      }
    } catch {
      case e: Exception => Left(e)
    }
  }
}
