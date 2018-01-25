package com.unilever.ohub.spark.generic

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions._
import StringFunctions._
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.types.StructType

import scala.collection.SortedSet

object SparkFunctions extends App {
  def readJdbcTable(spark: SparkSession, dbConnectionString: String = "jdbc:postgresql://localhost:5432/", dbName: String = "ufs_example", dbTable: String, userName: String = "ufs_example", userPassword: String = "ufs_example"): DataFrame = {
    val dbFullConnectionString = if (dbConnectionString.endsWith("/")) s"$dbConnectionString$dbName" else s"$dbConnectionString/$dbName"
    val jdbcProperties = new Properties
    jdbcProperties.put("user", userName)
    jdbcProperties.put("password", userPassword)
    spark.read.jdbc(dbFullConnectionString, dbTable, jdbcProperties)
  }

  private def testGetUniqueChars(): Unit = {
    val spark = SparkSession.builder().appName("SparkFunctions").getOrCreate()
    val FILE_NAME = "C:\\out\\CONTACTPERSONS.parquet"
    val FIELD_NAME = "LAST_NAME"
    val inputDF = spark.read.parquet(FILE_NAME)
    import spark.implicits._
    val outputDF = addIdFieldToDataFrameField(spark, inputDF, FIELD_NAME)
    outputDF.printSchema()
    val dataFrameSize = inputDF.count()
    val PART = 200000
    val partitions = List.range(PART, dataFrameSize, PART)
    var uniqueCharSet = scala.collection.mutable.SortedSet[Char]()
    partitions.foreach(part =>
      uniqueCharSet ++= getSetOfUniqueCharsInDataFrameColumn(outputDF.where("id between " + (part - PART) + " and " + part).select(FIELD_NAME)))
    println(uniqueCharSet)
  }

  def addIdFieldToDataFrameField(spark: SparkSession, dataFrame: DataFrame, fieldName: String): DataFrame = {
    import spark.implicits._
    val dataFramePart = dataFrame.select(fieldName)
    val dataFrameFull = dataFramePart.withColumn("ID", lit(1))
    val dataSet = dataFramePart.as[String]
    val rdd = dataSet.rdd
    val rddWithId = rdd.zipWithUniqueId()
    rddWithId.toDF(dataFrameFull.columns: _*)
  }

  def getSetOfUniqueCharsInDataFrameColumn(dataFrame: DataFrame): Set[Char] = {
    val DF = dataFrame.distinct()
    var myList = scala.collection.mutable.Set[String]()
    DF.collect().foreach(row => myList.add(row.toString().replace("[", "").replace("]", "")))
    collection.immutable.SortedSet(myList.mkString("").toCharArray.toList: _*)
  }

  def renameSparkCsvFileUsingHadoopFileSystem(spark:SparkSession, filePath:String, newFileName:String):Boolean = {
    import org.apache.hadoop.fs.{Path => hadoopPath}
    try{
      val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val originalPath = new hadoopPath(s"$filePath/part*")
      val file = fileSystem.globStatus(originalPath)(0).getPath
      fileSystem.rename(new hadoopPath(s"${file.getParent}/${file.getName}"), new hadoopPath(s"${file.getParent.getParent}/${newFileName}_$getFileDateString.csv"))
      fileSystem.delete(new hadoopPath(s"${file.getParent}"), true)
    } catch {
      case _: Exception => false
    }
    true
  }
}
