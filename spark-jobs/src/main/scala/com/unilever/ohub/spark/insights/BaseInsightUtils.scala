package com.unilever.ohub.spark.insights

import java.net.URI
import java.text.SimpleDateFormat
import java.util.{Calendar, UUID}

import com.unilever.ohub.spark.SparkJobConfig
import com.unilever.ohub.spark.storage.Storage
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.joda.time.format.DateTimeFormat
import org.joda.time.{LocalDate, Period}

import scala.collection.immutable.Stream.Empty
import scala.util.control.Breaks._



trait BaseInsightConfig extends DatabaseConfig {
  val incomingRawPath: String = "incomingRawPath"
  val previousInsightsPath: String = "previousInsightsPath"
  val insightsOutputPath: String = "insightsOutputPath"
  override val databaseUrl: String = "databaseUrl"
  override val databaseUserName: String = "databaseUserName"
  override val databasePassword: String = "databasePassword"
}

object BaseInsightUtils extends DatabaseUtils {

  val CSV_NAME_PATTERN_STRING= "UFS_([A-Z0-9_-]+)_" +
      "(CHAINS|LOYALTY|OPERATORS|OPERATOR_ACTIVITIES|OPERATOR_CLASSIFICATIONS|CONTACTPERSONS|CONTACTPERSON_ACTIVITIES|CONTACTPERSON_CLASSIFICATIONS|" +
      "CONTACTPERSON_SOCIAL|CONTACTPERSON_ANONYMIZATIONS|PRODUCTS|ORDERS|ORDERLINES|ORDERS_DELETED|ORDERLINES_DELETED|SUBSCRIPTIONS|QUESTIONS|ANSWERS|" +
      "CAMPAIGN_OPENS|CAMPAIGN_CLICKS|CAMPAIGN_BOUNCES|CAMPAIGNS|CAMPAIGN_SENDS|CHANNEL_MAPPINGS)" +
      "_([0-9]{14})"

  val CSV_NAME_PATTERN = CSV_NAME_PATTERN_STRING.r

  val getSourceName = udf((fileName: String) => getModelAndSourceName(fileName)._1)
  val getModelName  = udf((fileName: String) => getModelAndSourceName(fileName)._2)

  def getModelAndSourceName(csvFileName: String): (String, String) =  {
    csvFileName.toUpperCase() match {
      case CSV_NAME_PATTERN(sourceName,modelName,_) => (sourceName, modelName)
      case _ =>("-","-")
    }
  }

  def roundOffValue(input: Double): Double = {
    Math.round(input * 100.0) / 100.0
  }

  def getBaseName(path: String): String = {
    path.substring(path.lastIndexOf("/UFS_") + 1, path.lastIndexOf(".")).toUpperCase
  }

  def getAuditTrailsForDataCompletenessInsights(storage: Storage, config: BaseInsightConfig)(implicit spark: SparkSession) : DataFrame = {
    val query = InsightConstants.AUDIT_TRAILS_QUERY
    getAuditTrailData(storage, query, config)
  }

  def getAuditTrailsForFileUploadErrorsInsights(storage: Storage, config: BaseInsightConfig)(implicit spark: SparkSession) : DataFrame = {
    val query = InsightConstants.FILE_UPLOADS_QUERY
    getAuditTrailData(storage, query, config)
  }


  def getErrorsDataForFileUploadInsights(storage: Storage, config: BaseInsightConfig, filesList: Seq[String])(implicit spark: SparkSession) : DataFrame = {
    import spark.implicits._

    val errorTypeWindow = Window.partitionBy($"file_name", $"is_warning", $"split_error")
    val orderedErrorTypeWindow = errorTypeWindow.orderBy($"error_message")

    getErrorsData(storage, config)
      .filter($"file_name".isin(filesList:_*))
      .withColumn("split_error", substring_index($"error_message", " ", 2))
      .withColumn("error_count", count($"split_error") over errorTypeWindow)
      .withColumn("rowNumber", row_number over orderedErrorTypeWindow)
      .filter('rowNumber  === 1)
      .drop("line", "rowNumber", "rejected_value", "line_number", "timestamp", "split_error")
      .withColumnRenamed("file_name", "error_file_name")
      .withColumnRenamed("error_count", "ERROR_TYPE_COUNT_PER_FILE")
      .withColumnRenamed("error_message", "ERROR_MESSAGE")
      .withColumnRenamed("is_warning", "IS_WARNING")
  }

  def listCsvFiles(basePath: String, storage: Storage)(implicit spark: SparkSession): Seq[String] = {
    val conf = new Configuration(spark.sparkContext.hadoopConfiguration)
    val fs = FileSystem.get(new URI(basePath), conf)
    storage.getCsvFilePaths(fs, new Path(basePath))
      .map(_.toString)
      .filter(path => path.substring(InsightConstants.BASE_FILE_NAME_INDEX, path.length) matches InsightConstants.DATA_COMPLETENESS_FILE_PATTERN)
  }

  def listFiles(basePath: String, storage: Storage)(implicit spark: SparkSession): Seq[String] = {
    val conf = new Configuration(spark.sparkContext.hadoopConfiguration)
    val fs = FileSystem.get(new URI(basePath), conf)
    storage.getCsvFilePaths(fs, new Path(basePath))
      .map(_.toString)
      .filter(path => path.substring(InsightConstants.BASE_FILE_NAME_INDEX, path.length) matches InsightConstants.DATA_COMPLETENESS_FILE_PATTERN)
  }

  private def getDatesInRange(fromDate: String, toDate: String) = {

    def dateRange(from: LocalDate, to: LocalDate, step: Period): Iterator[LocalDate] =
      Iterator.iterate(from)(_.plus(step)).takeWhile(!_.isAfter(to))

    val dateFormat = DateTimeFormat.forPattern("yyyy-MM-dd")

    val fromDateLoc = LocalDate.parse(fromDate, dateFormat)
    val toDateLoc = LocalDate.parse(toDate, dateFormat)

    dateRange(fromDateLoc, toDateLoc, new Period().withDays(1)).map(_.toString)
  }

  def listIncomingFiles(basePath: String, startDate:String, endDate:String, storage: Storage)
                       (implicit spark: SparkSession): Seq[String] = {
    val dateList = getDatesInRange(startDate, endDate)
      .map(date => s"$basePath${date}")
      .toList

    var files = Seq("UFS_DUMMY_ROW.CSV")
    for (dayOne <- dateList) {
      breakable {
        try{
          files = files.union(listFiles(dayOne,storage))
        } catch {
          case ex: java.io.FileNotFoundException => break
        }
      }
    }

    files=dropFirstMatch(files,"UFS_DUMMY_ROW.CSV")
    files
  }

  def dropFirstMatch[A](ls: Seq[A], value: A): Seq[A] = {
    val index = ls.indexOf(value)  //index is -1 if there is no match
    if (index < 0) {
      ls
    } else if (index == 0) {
      ls.tail
    } else {
      // splitAt keeps the matching element in the second group
      val (a, b) = ls.splitAt(index)
      a ++ b.tail
    }
  }


  def writeToCsv(path: String, fileName: String, ds: Dataset[_])(implicit spark: SparkSession): Unit = {
    val outputFolderPath = new Path(path)
    val temporaryPath = new Path(outputFolderPath, UUID.randomUUID().toString)
    val outputFilePath = new Path(outputFolderPath, s"${fileName}")
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

  def getLatestEngineRunDates():String = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal = Calendar.getInstance()

    cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    val dates=dateFormat.format(cal.getTime())
    dates
  }

  def getParentOne(child: String): String = {
    child match {
      case "CONTACTPERSONS" => "operators"
      case "ORDERLINES" => "orders"
      case "SUBSCRIPTION" => "contactpersons"
      case "LOYALTY" => "operators"
      case "ORDERS" => "operators"
      case "OPERATOR_ACTIVITIES" => "operators"
      case "CONTACTPERSON_ACTIVITIES" => "contactpersons"
      case _=>""
    }
  }

  def getParentTwo(child: String): String = {
    child match {
      case "ORDERLINES" => "products"
      case "ORDERS" => "contactpersons"
      case "LOYALTY" => "contactpersons"
      case _=>""
    }
  }

  def getCampaignParent(child: String): String = {
    child match {
      case "CAMPAIGNS" => "contactpersons"
      case "CAMPAIGN_SENDS" => "contactpersons"
      case "CAMPAIGN_CLICKS" => "contactpersons"
      case "CAMPAIGN_OPENS" => "contactpersons"
      case "CAMPAIGN_BOUNCES" => "contactpersons"
      case _=>""
    }
  }

  def getParentModel(child:String, category:Int):String = {
    category match {
      case 1 => getParentOne(child)
      case 2 => getParentTwo(child)
      case 3 => getCampaignParent(child)
    }
  }

}
