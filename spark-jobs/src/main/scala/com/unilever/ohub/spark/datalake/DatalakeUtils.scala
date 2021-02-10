package com.unilever.ohub.spark.datalake

import java.text.SimpleDateFormat

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import java.util.{Calendar, UUID}

import com.unilever.ohub.spark.insights.{DatabaseConfig, DatabaseUtils}
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.date_format
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer

trait DataLakeConfig extends DatabaseConfig  {
  val country: String = " country_code"
  val previousIntegratedPath: String = "previous integrated file path"
  val outputPath: String = "output file path"
  val fromDate: String = "errors to fetch from date"
  val toDate: Option[String] = None
  override val databaseUrl: String = "databaseUrl"
  override val databaseUserName: String = "databaseUserName"
  override val databasePassword: String = "databasePassword"
}

object DatalakeUtils extends DatabaseUtils {

  def bulkListLeafFiles(conf: Configuration, fs: FileSystem, spark: SparkSession, basep: String): Seq[String] = {
    val status = fs.listStatus(new Path(basep))
    status.map(_.getPath.toString).filter(_.split('.').last == "csv")
  }


  def writeToCsv(path: String, fileName: String, ds: Dataset[_], scfile:RDD[String], spark: SparkSession): Unit = {
    if(!scfile.first().contains(",")) {
      val outputFolderPath = new Path(path)
      val temporaryPath = new Path(outputFolderPath, UUID.randomUUID().toString)
      val outputFilePath = new Path(outputFolderPath, s"$fileName")

      val writeableData = ds
        .write
        .mode(SaveMode.Overwrite)
        .option("encoding", "UTF-8")
        .option("header", "false")
        .option("quoteAll", "true")
        .option("delimiter", ";")

      writeableData.csv(temporaryPath.toString)
      val header = ds.columns.map(c ⇒ "\"" + c + "\"").mkString(";")
      mergeDirectoryToOneFile(temporaryPath, outputFilePath, spark, header)
    }
  }

  //scalastyle:off
  def writeToCsv(path: String, fileName: String, ds: Dataset[_], spark: SparkSession): Unit = {
    val outputFolderPath = new Path(path)
    val temporaryPath = new Path(outputFolderPath, UUID.randomUUID().toString)
    val outputFilePath = new Path(outputFolderPath, s"$fileName")
    val nullValue = null
    val writeableData = ds
      .write
      .mode("Overwrite")
      .option("encoding", "UTF-8")
      .option("header", "false")
      .option("quoteAll", "false")
//      .option("quote", "\"")
      .option("delimiter", ";")
      .option("escape", "\\")
      .option("nullValue",nullValue)

    writeableData.csv(temporaryPath.toString)
    val header = ds.columns.map(c ⇒ c ).mkString(";")
    mergeDirectoryToOneFile(temporaryPath, outputFilePath, spark, header)
  }

  def mergeDirectoryToOneFile(sourceDirectory: Path, outputFile: Path, spark: SparkSession, header: String): Boolean = {

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    //Create a new header file which start with a `_` because the files are merged based on alphabetical order so
    //the header files will be merged first
    createHeaderFile(fs, sourceDirectory, header)


    def moveOnlyCsvFilesToOtherDirectory = {
      //Move all csv files to different directory so we don't make a mistake of merging other files from the source directory
      val tmpCsvSourceDirectory = new Path(sourceDirectory.getParent, UUID.randomUUID().toString)
      fs.mkdirs(tmpCsvSourceDirectory)
      fs.listStatus(sourceDirectory)
        .filter(p ⇒ p.isFile)
        .filter(p ⇒ p.getPath.getName.endsWith(".csv"))
        .map(_.getPath)
        .foreach(fs.rename(_, tmpCsvSourceDirectory))
      tmpCsvSourceDirectory
    }

    val tmpCsvSourceDirectory: Path = moveOnlyCsvFilesToOtherDirectory
    fs.delete(outputFile, true) // This is to delete any existing file that is present there. Its an indirect overwrite
    FileUtil.copyMerge(fs, tmpCsvSourceDirectory, fs, outputFile, true, spark.sparkContext.hadoopConfiguration, null) // scalastyle:ignore
    fs.delete(sourceDirectory, true)
  }

  def createHeaderFile(fs: FileSystem, sourceDirectory: Path, header: String): Path = {
    import java.io.{BufferedWriter, OutputStreamWriter}

    val headerFile = new Path(sourceDirectory, "_" + UUID.randomUUID().toString + ".csv")
    val out = fs.create(headerFile)
    val br = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"))
    try {
      br.write(header + "\n")
    } finally {
      if (out != null) br.close()
    }
    headerFile
  }

  def getCountryBasedUploadErrors(storage: Storage, config: DataLakeConfig)(implicit spark: SparkSession) : DataFrame = {
    import spark.implicits._

    DatalakeUtils.getErrorsData(storage,config)
        .filter(date_format($"timestamp", "yyyy-MM-dd") >= config.fromDate)
        .filter(date_format($"timestamp", "yyyy-MM-dd") <= config.toDate.getOrElse(config.fromDate))
        .filter($"country_code" === config.country)
  }

  def getFolderDateList(fromDate:String, toDate:String): List[String] = {

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val dateFiled = Calendar.DAY_OF_MONTH
    var dateFrom = dateFormat.parse(fromDate)
    val dateTo = dateFormat.parse(toDate)
    val calendar = Calendar.getInstance()

    calendar.setTime(dateFrom)
    val dateArray: ArrayBuffer[String] = ArrayBuffer()
    while (dateFrom.compareTo(dateTo) <= 0) {
      dateArray += dateFormat.format(dateFrom)
      calendar.add(dateFiled, 1)
      dateFrom = calendar.getTime}
    dateArray.toList
  }
}
