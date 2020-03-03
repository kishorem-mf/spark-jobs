package com.unilever.ohub.spark.insights


import org.apache.spark.sql.functions._
import com.unilever.ohub.spark.{SparkJob, SparkJobConfig}
import com.unilever.ohub.spark.datalake.DatalakeUtils
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import scopt.OptionParser

case class DataCompletenessConfig(
                                 override val incomingRawPath: String = "incomingRawPath",
                                 override val previousInsightsPath: String = "previousInsightsPath",
                                 override val insightsOutputPath: String = "insightsOutputPath",
                                 override val databaseUrl: String = "databaseUrl",
                                 override val databaseUserName:String = "databaseUserName",
                                 override val databasePassword:String = "databasePassword"
                                 ) extends BaseInsightConfig

object DataCompleteness extends SparkJob[DataCompletenessConfig] {

  override private[spark] def defaultConfig = DataCompletenessConfig()

  override private[spark] def configParser(): OptionParser[DataCompletenessConfig] =
    new scopt.OptionParser[DataCompletenessConfig]("get Data Completeness Insights") {
      head("change log entity output file.", "1.0")
      opt[String]("incomingRawPath") required() action { (x, c) ⇒
        c.copy(incomingRawPath = x)
      } text("Path of Incoming blob folder")
      opt[String]("previousInsightsPath") required() action { (x, c) ⇒
        c.copy(previousInsightsPath = x)
      } text("Previous insight blob file path")
      opt[String]("insightsOutputPath") required() action { (x, c) ⇒
        c.copy(insightsOutputPath = x)
      } text("Output blob path")
      opt[String]("databaseUrl") required() action { (x, c) ⇒
        c.copy(databaseUrl = x)
      } text("URL of database")
      opt[String]("databaseUserName") required() action { (x, c) ⇒
        c.copy(databaseUserName = x)
      } text("Username of database")
      opt[String]("databasePassword") required() action { (x, c) ⇒
        c.copy(databasePassword = x)
      } text("Password of Database")

      version("1.0")
      help("help") text "help text"
    }

  def getDataFilledPercentage(incomingDF: DataFrame)(implicit spark: SparkSession): Double = {

    import spark.implicits._

    val columnCount = incomingDF.columns.filterNot(_.startsWith("_c")).size
    val totalRowCount = incomingDF.count
    val totalCellCount = columnCount * totalRowCount

    val notNullCountDF = incomingDF.describe().filter($"summary" === "count").drop("summary")
    val columnList: List[Column] = notNullCountDF.columns.map(col).toList
    val sum = notNullCountDF.withColumn("countTotal", columnList.reduce(_ + _))
    val filledCount = sum.select("countTotal").as[Double].collect()

    val percentage = (filledCount(0)/totalCellCount*100)
    BaseInsightUtils.roundOffValue(percentage)
  }

  def getFileInsights(incomingFilePath: String, storage: Storage)(implicit spark: SparkSession): (String,String, String, Long, Double, Double) = {

    val contactPersonFpoFields = InsightConstants.CONTACTPERSON_FPO_FIELDS
    val operatorFpoFields = InsightConstants.OPERATOR_FPO_FIELDS

    try{
      val baseFileName = BaseInsightUtils.getBaseName(incomingFilePath)
      val incomingFileDF = storage.readFromCsv(incomingFilePath, InsightConstants.SEMICOLON).cache
      val totalRowCount = incomingFileDF.count

      val (sourceName, modelName) = BaseInsightUtils.getModelAndSourceName(baseFileName)

      val dataFilledPercentage = getDataFilledPercentage(incomingFileDF)
      val fpoFilledPercentage = modelName match {
        case "OPERATORS" => getDataFilledPercentage(incomingFileDF.select(operatorFpoFields.map(col): _*))
        case "CONTACTPERSONS" => getDataFilledPercentage(incomingFileDF.select(contactPersonFpoFields.map(col): _*))
        case _ => 0
      }

      (baseFileName, modelName, sourceName, totalRowCount, dataFilledPercentage, fpoFilledPercentage)

    }catch {
      case e:  Exception => log.debug(s"Issue in file :: ${incomingFilePath}")
      (incomingFilePath, "-", "-", 0L, 0d, 0d)
    }
  }

  def transform(config: DataCompletenessConfig, storage: Storage)(implicit spark: SparkSession) : DataFrame = {

    import spark.implicits._
    val dataCompletenessColumns = InsightConstants.DATA_COMPLETENESS_COLUMNS
    val auditTrailsDF = BaseInsightUtils.getAuditTrailsForDataCompletenessInsights(storage, config)

    val fileInsightDF = BaseInsightUtils.listCsvFiles(config.incomingRawPath, storage)
      .par
      .map(filePath => getFileInsights(filePath, storage))
      .seq
      .toDF("FILE_NAME", "MODEL", "SOURCE", "TOTAL_ROW_COUNT", "DATA_FILLED_PERCENTAGE", "FPO_DATA_FILLED_PERCENTAGE")
      .cache

    fileInsightDF
      .join(auditTrailsDF, fileInsightDF("FILE_NAME") === auditTrailsDF("AUDIT_FILE_NAME"), JoinType.Left)
      .drop("AUDIT_FILE_NAME")
      .select(dataCompletenessColumns.head, dataCompletenessColumns.tail: _*)
  }

  override def run(spark: SparkSession, config: DataCompletenessConfig, storage: Storage): Unit = {

    implicit val sparkSession:SparkSession = spark

    val previousIntegratedInsightsDF = storage.readFromCsv(config.previousInsightsPath, InsightConstants.SEMICOLON).cache()

    val deltaIntegratedInsightsDF = transform(config, storage)

    val integratedDF = deltaIntegratedInsightsDF.unionByName(previousIntegratedInsightsDF)

    BaseInsightUtils.writeToCsv(config.insightsOutputPath, InsightConstants.DATA_COMPLETENESS_FILENAME, integratedDF)

  }
}
