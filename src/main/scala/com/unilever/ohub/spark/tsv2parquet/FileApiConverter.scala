package com.unilever.ohub.spark.tsv2parquet

import java.io.File

import scala.io
import org.apache.spark.sql.SparkSession

import scala.io.StdIn

  case class icdRecord(FILE: String,NR: Int,FIELD: String,MANDATORY: String,MAX_LENGTH: Int,FORMAT: String,EXPLANATION: String,EXAMPLE: String,CHECK: String)
  case class sourcesRecord(SOURCE: String)

object FileApiConverter {

  var fullInputFolderPath: String = _
  var fullOutputFolderPath: String = _
  var fullHelpFolderPath: String = _
  var inputLine: String = _

  val spark: SparkSession = builtSparkSqlSession("Convert Files")
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    while (fullInputFolderPath == null) {
      println("Please type in the absolute folder path of your input files: ")
      inputLine = StdIn.readLine()
      fullInputFolderPath = inputLine
    }
    inputLine = null
    while (fullOutputFolderPath == null) {
      println("Please type in the absolute folder path of your output files: ")
      inputLine = StdIn.readLine()
      fullOutputFolderPath = inputLine
    }
    inputLine = null
    while (fullHelpFolderPath == null) {
      println("Please type in the absolute folder path to the icd.txt and sources.txt files: ")
      inputLine = StdIn.readLine()
      fullHelpFolderPath = inputLine
    }
    val listOfInputFiles: List[File] = getListOfFiles(fullInputFolderPath)
    val listOfHelpFiles: List[File] = getListOfFiles(fullHelpFolderPath)
    val inputFilesPath = listOfInputFiles.filter(_.getName.contains(".csv")).head.getAbsolutePath
    val icdFilePath = listOfInputFiles.filter(_.getName.contains("icd.txt")).head.getAbsolutePath
    val sourcesFilePath = listOfInputFiles.filter(_.getName.contains("sources.txt")).head.getAbsolutePath
    val inputLines = spark.read.textFile(inputFilesPath).as[String]

    val icdLines = spark.read.textFile(icdFilePath).as[String]
    val icdRecords = icdLines
      .filter(line => line != icdLines.first())
      .map(line => line.split("~"))
      .map(cells => icdRecord(FILE = cells(0),NR = cells(1).toInt,FIELD = cells(2),MANDATORY = cells(3),MAX_LENGTH = cells(4).toInt,FORMAT = cells(5),EXPLANATION = cells(6),EXAMPLE = cells(7),CHECK = cells(8)))
    icdRecords.createOrReplaceTempView("ICD")
    val icdSql = spark.sql("SELECT * FROM ICD")
    icdSql.show()

    val sourcesLines = spark.read.textFile(sourcesFilePath).as[String]
    val sourcesRecords = sourcesLines
      .filter(line => !line.isEmpty)
      .map(cell => sourcesRecord(SOURCE = cell))

    val firstInputLine: String = inputLines.first()
    val delimiterArray = Array(firstInputLine.count(_ == ';'), firstInputLine.count(_ == ','), firstInputLine.count(_ == '|'), firstInputLine.count(_ == '‰'), firstInputLine.count(_ == '\t'),firstInputLine.count(_ == '~'),firstInputLine.count(_ == '¶'))
    val arrayIndex: Int = 1 + delimiterArray.indexOf(delimiterArray.max)
    var thisDelimiter: String = null
    arrayIndex match
    {
      case 1 => thisDelimiter = ";"
      case 2 => thisDelimiter = ","
      case 3 => thisDelimiter = "|"
      case 4 => thisDelimiter = "‰"
      case 5 => thisDelimiter = "\\t"
      case 6 => thisDelimiter = "~"
      case 7 => thisDelimiter = "¶"
    }
    val inputRecords = inputLines
      .filter(line => !line.isEmpty)
      .map(line => line.split(thisDelimiter))
//      .map(cells => {

//      })


    /*.map(_.split("‰"))*/
    println("stop")
  }

  def builtSparkSqlSession(thisAppName: String): SparkSession = {
    val spark = SparkSession.builder().appName(thisAppName).getOrCreate()
    spark
  }

  def getListOfFiles(fullFolderPath: String): List[File] = {
    val thisDirectory = new File(fullFolderPath)
    if (thisDirectory.exists && thisDirectory.isDirectory) {
        thisDirectory.listFiles.filter(_.isFile).toList
    } else {
        List[File]()
    }
  }
//  val spark = SparkSession.builder().appName("test").config("spark.master", "local").getOrCreate()
}


