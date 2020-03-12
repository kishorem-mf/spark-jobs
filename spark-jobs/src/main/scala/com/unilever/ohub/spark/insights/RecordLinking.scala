package com.unilever.ohub.spark.insights


import org.apache.spark.sql.functions._
import com.unilever.ohub.spark.{SparkJob, SparkJobConfig}
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql._
import scopt.OptionParser


case class RecordLinkingConfig(
                                override val incomingRawPath: String = "incomingRawPath",
                                override val previousInsightsPath: String = "previousInsightsPath",
                                override val insightsOutputPath: String = "insightsOutputPath",
                                override val databaseUrl: String = "databaseUrl",
                                override val databaseUserName:String = "databaseUserName",
                                override val databasePassword:String = "databasePassword",
                                insightsExecdate: String = "insightsExecdate",
                                startDate:String = "startDate",
                                endDate:String = "endDate"
                              ) extends BaseInsightConfig


object RecordLinking extends SparkJob[RecordLinkingConfig] {

  override private[spark] def defaultConfig = RecordLinkingConfig()

  override private[spark] def configParser(): OptionParser[RecordLinkingConfig] =
    new scopt.OptionParser[RecordLinkingConfig]("get Data RecordLinking Insights") {
      head("change log entity output file.", "1.0")
      opt[String]("incomingRawPath") required() action { (x, c) ⇒
        c.copy(incomingRawPath = x)
      } text "Path of Incoming blob folder"
      opt[String]("previousInsightsPath") required() action { (x, c) ⇒
        c.copy(previousInsightsPath = x)
      } text "Previous insight blob file path"
      opt[String]("insightsOutputPath") required() action { (x, c) ⇒
        c.copy(insightsOutputPath = x)
      } text "Output blob path"
      opt[String]("insightsExecdate") required() action { (x, c) ⇒
        c.copy(insightsExecdate = x)
      } text "exec date"
      opt[String]("databaseUrl") required() action { (x, c) ⇒
        c.copy(databaseUrl = x)
      } text "Url of Database"
      opt[String]("databaseUserName") required() action { (x, c) ⇒
        c.copy(databaseUserName = x)
      } text "Username of Database"
      opt[String]("databasePassword") required() action { (x, c) ⇒
        c.copy(databasePassword = x)
      } text "Password of Database"
      opt[String]("startDate") required() action { (x, c) ⇒
        c.copy(startDate = x)
      } text "start Date"
      opt[String]("endDate") required() action { (x, c) ⇒
        c.copy(endDate = x)
      } text "end Date"
    }

  def getRecordLinkingInsightsParentOne(incomingFilePath: String, engineRunDate:String, storage: Storage)
                                       (implicit spark: SparkSession): (String,Double,Double,String, String) = {

    val baseFileName = BaseInsightUtils.getBaseName(incomingFilePath)
    try {
      val incomingFileDF = storage.readFromCsv(incomingFilePath, InsightConstants.SEMICOLON).cache

      val (sourceName, modelName) = BaseInsightUtils.getModelAndSourceName(baseFileName)
      log.info(s"sourceName: [$sourceName] modelName: [$modelName] baseFileName: [$baseFileName]")

      val (isLinked: Double, isNotLinked: Double) = modelName match {
        case "CONTACTPERSONS" => contactPersons(engineRunDate, incomingFileDF, sourceName, modelName)
        case "ORDERLINES" => orderLinesParentOne(engineRunDate, incomingFileDF, sourceName, modelName)
        case "SUBSCRIPTION" => getRecordLinkings(incomingFileDF.select("CONTACT_PERSON_REF_ID"), engineRunDate, modelName, sourceName)
        case "LOYALTY" => loyalty(engineRunDate, incomingFileDF, sourceName, modelName)
        case "ORDERS" => ordersParentOne(engineRunDate, incomingFileDF, sourceName, modelName)
        case "OPERATOR_ACTIVITIES" =>  operatorActivities(engineRunDate, incomingFileDF, sourceName, modelName)
        case "CONTACTPERSON_ACTIVITIES" =>  contactPersonActivities(engineRunDate, incomingFileDF, sourceName, modelName)
      }
      (baseFileName, isLinked, isNotLinked, sourceName, modelName)

    } catch {
      case e: Exception => log.info(s"Issue in file :: $incomingFilePath Exception :: ${e.toString}")
        (incomingFilePath, 0L, 0L, "_", "_")
    }

  }

  def getRecordLinkingInsightsCampaigns(incomingFilePath: String, engineRunDate: String, storage: Storage)
                                       (implicit spark: SparkSession): (String,Double,Double,String, String) = {
    val baseFileName = BaseInsightUtils.getBaseName(incomingFilePath)
    try {
      val incomingFileDF = storage.readFromCsv(incomingFilePath, InsightConstants.SEMICOLON).cache

      val (sourceName, modelName) = BaseInsightUtils.getModelAndSourceName(baseFileName)
      val (isLinked: Double, isNotLinked: Double) = modelName match {
        case "CAMPAIGNS" => getRecordLinkings(incomingFileDF.select("Contact ID"), engineRunDate, modelName, sourceName,3)
        case "CAMPAIGN_SENDS" => getRecordLinkings(incomingFileDF.select("Contact ID"), engineRunDate, modelName, sourceName,3)
        case "CAMPAIGN_CLICKS" => getRecordLinkings(incomingFileDF.select("Contact ID"), engineRunDate, modelName, sourceName,3)
        case "CAMPAIGN_OPENS" => getRecordLinkings(incomingFileDF.select("Contact ID"), engineRunDate, modelName, sourceName,3)
        case "CAMPAIGN_BOUNCES" => getRecordLinkings(incomingFileDF.select("Contact ID"), engineRunDate, modelName, sourceName,3)
      }
      (baseFileName, isLinked, isNotLinked, sourceName, modelName)

    } catch {
      case e: Exception => log.info(s"Issue in file :: $incomingFilePath Exception :: ${e.toString}")
        (incomingFilePath, 0L, 0L, "_", "_")
    }
  }

  def transform(config: RecordLinkingConfig, storage: Storage)(implicit spark: SparkSession) : DataFrame = {

        import spark.implicits._
        val auditTrailsDF = BaseInsightUtils.getAuditTrailsForDataCompletenessInsights(storage, config)

        val fileInsightDFOne = BaseInsightUtils.listIncomingFiles(config.incomingRawPath, config.startDate, config.endDate, storage)
          .par
          .map(filePath => getRecordLinkingInsightsParentOne(filePath,BaseInsightUtils.getLatestEngineRunDates(), storage))
          .seq
          .toDF("filename","isLinked","isNotLinked","Source","Entity")
          .cache
        val fileInsightDFTwo = BaseInsightUtils.listIncomingFiles(config.incomingRawPath, config.startDate, config.endDate, storage)
          .par
          .map(filePath => getRecordLinkingInsightsParentTwo(filePath,BaseInsightUtils.getLatestEngineRunDates(), storage))
          .seq
          .toDF("filename","isLinked","isNotLinked","Source","Entity")
          .cache
        val campaignLinkingInsights = BaseInsightUtils.listIncomingFiles(config.incomingRawPath, config.startDate, config.endDate, storage)
          .par
          .map(filePath => getRecordLinkingInsightsCampaigns(filePath,BaseInsightUtils.getLatestEngineRunDates(), storage))
          .seq
          .toDF("filename","isLinked","isNotLinked","Source","Entity")
          .cache

        val combined = fileInsightDFOne.unionByName(fileInsightDFTwo).unionByName(campaignLinkingInsights)

        combined
          .join(auditTrailsDF, combined("filename") === auditTrailsDF("AUDIT_FILE_NAME"), JoinType.Left)
          .drop("AUDIT_FILE_NAME")
      }

      def getRecordLinkings(incomingDF: DataFrame, engineRunDate:String, model:String, source:String, category:Int=1)(implicit spark: SparkSession): (Double,Double) = {
        import spark.implicits._
        val parentModel=BaseInsightUtils.getParentModel(model,category)

        val integ_parent = spark.read.parquet(s"dbfs:/mnt/engine/integrated/$engineRunDate/$parentModel.parquet").select("sourceEntityId")

        val intermediateDf = incomingDF

        var with_link=intermediateDf.intersect(integ_parent)
        with_link=with_link.withColumn("isLinked",lit(1))
        with_link=with_link.withColumn("isNotLinked",lit(0))
        with_link=with_link.dropDuplicates()

        var with_no_link=intermediateDf.except(integ_parent)
        with_no_link=with_no_link.withColumn("isLinked",lit(0))
        with_no_link=with_no_link.withColumn("isNotLinked",lit(1))
        with_no_link=with_no_link.dropDuplicates()

        (with_link.filter($"isLinked"===1).count(),with_link.filter($"isNotLinked"===1).count())
      }


      def getRecordLinkingInsightsParentTwo(incomingFilePath: String, engineRunDate:String, storage: Storage)(implicit spark: SparkSession): (String,Double,Double,String, String) = {
        val baseFileName = BaseInsightUtils.getBaseName(incomingFilePath)
        try {
          val incomingFileDF = storage.readFromCsv(incomingFilePath, InsightConstants.SEMICOLON).cache

          val (sourceName, modelName) = BaseInsightUtils.getModelAndSourceName(baseFileName)
          log.info(s"sourceName: [$sourceName] modelName: [$modelName] baseFileName: [$baseFileName]")

          val (isLinked: Double, isNotLinked: Double) = modelName match {
            case "ORDERLINES" => orderLinesParentTwo(engineRunDate, incomingFileDF, sourceName, modelName)
            case "LOYALTY" =>  getRecordLinkings(incomingFileDF.select("CONTACT_PERSON_REF_ID"),
              engineRunDate, modelName, sourceName, 2)
            case "ORDERS" => orderLinesParentTwo(engineRunDate, incomingFileDF, sourceName, modelName)
          }
          (baseFileName, isLinked, isNotLinked, sourceName, modelName)

        } catch {
          case e: Exception => log.info(s"Issue in file :: $incomingFilePath Exception :: ${e.toString}")
            (incomingFilePath, 0L, 0L, "_", "_")
        }
      }

  private def loyalty(engineRunDate: String, incomingFileDF: Dataset[Row], sourceName: String, modelName: String)(implicit spark: SparkSession) = {
    getRecordLinkings(incomingFileDF.select("OPERATOR_REF_ID"), engineRunDate, modelName, sourceName)
  }

  private def contactPersons(engineRunDate: String, incomingFileDF: Dataset[Row], sourceName: String, modelName: String)(implicit spark: SparkSession) = {
    sourceName match {
      case "DEX" => getRecordLinkings(incomingFileDF.select("Account_ID"), engineRunDate, modelName, sourceName)
      case _ => getRecordLinkings(incomingFileDF.select("REF_OPERATOR_ID"), engineRunDate, modelName, sourceName)
    }
  }


  private def contactPersonActivities(engineRunDate: String, incomingFileDF: Dataset[Row], sourceName: String, modelName: String)(implicit spark: SparkSession) = {
    sourceName match {
      case "OHUB1" => getRecordLinkings(incomingFileDF.select("CONTACT_PERSON_REF_ID"), engineRunDate, modelName, sourceName)
      case _ => getRecordLinkings(incomingFileDF.select("REF_CONTACT_PERSON_ID"), engineRunDate, modelName, sourceName)
    }
  }

  private def operatorActivities(engineRunDate: String, incomingFileDF: Dataset[Row], sourceName: String, modelName: String)(implicit spark: SparkSession) = {
    sourceName match {
      case "OHUB1" => getRecordLinkings(incomingFileDF.select("OPERATOR_REF_ID"), engineRunDate, modelName, sourceName)
      case _ => getRecordLinkings(incomingFileDF.select("REF_OPERATOR_ID"), engineRunDate, modelName, sourceName)
    }
  }

  private def ordersParentOne(engineRunDate: String, incomingFileDF: Dataset[Row], sourceName: String, modelName: String)(implicit spark: SparkSession) = {
    sourceName match {
      case "DEX" =>
        getRecordLinkings(incomingFileDF.select("AccountMRDR"), engineRunDate, modelName, sourceName)
      case "OHUB1" =>
        getRecordLinkings(incomingFileDF.select("OPERATOR_REF_ID"), engineRunDate, modelName, sourceName)
      case "FUZZIT" =>
        getRecordLinkings(incomingFileDF.select("CustUID"), engineRunDate, modelName, sourceName)
      case _ =>
        getRecordLinkings(incomingFileDF.select("REF_OPERATOR_ID"), engineRunDate, modelName, sourceName)
    }
  }

  private def ordersParentTwo(engineRunDate: String, incomingFileDF: Dataset[Row], sourceName: String, modelName: String)(implicit spark: SparkSession) = {
    sourceName match {
      case "DEX" =>
        getRecordLinkings(incomingFileDF.select("Ref_Contact_Person_ID"), engineRunDate, modelName, sourceName, 2)
      case "OHUB1" =>
        getRecordLinkings(incomingFileDF.select("CONTACT_PERSON_REF_ID"), engineRunDate, modelName, sourceName, 2)
      case _ =>
        getRecordLinkings(incomingFileDF.select("REF_CONTACT_PERSON_ID"), engineRunDate, modelName, sourceName, 2)
    }
  }

  private def orderLinesParentOne(engineRunDate: String, incomingFileDF: Dataset[Row], sourceName: String, modelName: String)(implicit spark: SparkSession) = {
    sourceName match {
      case "DEX" =>
        getRecordLinkings(incomingFileDF.select("Transaction_Number"), engineRunDate, modelName, sourceName)
      case "OHUB1" =>
        getRecordLinkings(incomingFileDF.select("SOURCE_ENTITY_ID"), engineRunDate, modelName, sourceName)
      case "FUZZIT" =>
        getRecordLinkings(incomingFileDF.select("SalesLineUID"), engineRunDate, modelName, sourceName)
      case _ =>
        getRecordLinkings(incomingFileDF.select("REF_ORDER_ID"), engineRunDate, modelName, sourceName)
    }
  }

  private def orderLinesParentTwo(engineRunDate: String, incomingFileDF: Dataset[Row], sourceName: String, modelName: String)(implicit spark: SparkSession) = {
    sourceName match {
      case "DEX" =>
        getRecordLinkings(incomingFileDF.select("EAN_Code"), engineRunDate, modelName, sourceName, 2)
      case "OHUB1" =>
        getRecordLinkings(incomingFileDF.select("PRODUCT_REF_ID"), engineRunDate, modelName, sourceName, 2)
      case "FUZZIT" =>
        getRecordLinkings(incomingFileDF.select("MatUID"), engineRunDate, modelName, sourceName, 2)
      case _ =>
        getRecordLinkings(incomingFileDF.select("REF_PRODUCT_ID"), engineRunDate, modelName, sourceName, 2)
    }
  }

      override def run(spark: SparkSession, config: RecordLinkingConfig, storage: Storage): Unit = {

        implicit val sparkSession:SparkSession = spark

        val previousIntegratedInsightsDF = storage.readFromCsv(config.previousInsightsPath, InsightConstants.SEMICOLON).cache()

        val deltaIntegratedInsightsDF = transform(config, storage)

        val integratedDF = deltaIntegratedInsightsDF.unionByName(previousIntegratedInsightsDF)

        BaseInsightUtils.writeToCsv(config.insightsOutputPath, InsightConstants.RECORD_LINKING_FILENAME, integratedDF)

      }
}


