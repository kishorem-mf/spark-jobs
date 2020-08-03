package com.unilever.ohub.spark.export.acm

import com.unilever.ohub.spark.domain.DomainEntityUtils
import com.unilever.ohub.spark.domain.entity._
import com.unilever.ohub.spark.export.acm.model.{AcmContactPerson, _}
import com.unilever.ohub.spark.export.{CsvOptions, ExportOutboundWriter, OutboundConfig, SparkJobWithOutboundExportConfig}
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{Column, Dataset, SparkSession}
import org.apache.spark.sql.functions.{lit, when}

trait AcmOptions extends CsvOptions {

  override val delimiter: String = "\u00B6"

  override val extraOptions = Map(
    "delimiter" -> delimiter
  )

}

object ContactPersonOutboundWriter extends ExportOutboundWriter[ContactPerson] with AcmOptions {

  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[ContactPerson]) = {
    import spark.implicits._
    dataSet.map(ContactPersonAcmConverter.convert(_))
  }

  override private[export] def filterValid[AcmContactPerson](spark: SparkSession, dataSet: Dataset[_], config: OutboundConfig) = {

    import spark.implicits._

    dataSet
      .filter(($"emailAddress".isNotNull && $"emailAddress" =!= "") || ($"mobileNumber".isNotNull && $"mobileNumber" =!= ""))
      .withColumn("isActive",
        when(($"isEmailAddressValid" === lit(false) && ($"mobileNumber".isNull || $"mobileNumber" === "")) ||
          ($"isMobileNumberValid" === lit(false) && ($"emailAddress".isNull || $"emailAddress" === "")),
          lit(false)).otherwise($"isActive"))
      .withColumn("isActive", when( ($"isMobileNumberValid" === lit(false) && $"isEmailAddressValid" === lit(false)),
        lit(false)) otherwise($"isActive"))
  }

  override def explainConversion: Option[ContactPerson => AcmContactPerson] = Some((input: ContactPerson) => ContactPersonAcmConverter.convert(input, true))

  override def entityName(): String = "RECIPIENTS"

  override def run(spark: SparkSession, config: OutboundConfig, storage: Storage): Unit = {
    import spark.implicits._

    log.info(
      s" For CP writing integrated entities ::   [${config.integratedInputFile}] " +

        s"with parameters currentMerged :: [${config.currentMerged}]" +
        s"with parameters previousMerged:: [${config.previousMerged}]" +
        s"with parameters prevIntegrated:: [${config.previousIntegratedInputFile}]" +
        s"with parameters currentMergedOPR:: [${config.currentMergedOPR}]" +
        s"to outbound export csv file for ACM and DDB.")

    val previousIntegratedFile = config.previousIntegratedInputFile.fold(spark.createDataset[ContactPerson](Nil))(storage.readFromParquet[ContactPerson](_))
    val currentIntegrated = storage.readFromParquet[ContactPerson](config.integratedInputFile)
    val currentMerged = config.currentMerged.fold(spark.createDataset[ContactPerson](Nil))(storage.readFromParquet[ContactPerson](_))
    val previousMerged = config.previousMerged.fold(spark.createDataset[ContactPerson](Nil))(storage.readFromParquet[ContactPerson](_))
    val deletedOhubID = getDeletedOhubIdsWithTargetId(spark, previousIntegratedFile, currentIntegrated, previousMerged, currentMerged)
    val currentMergedOPR = config.currentMergedOPR.fold(spark.emptyDataFrame)(spark.read.parquet(_))

    export(
      currentIntegrated,
      deletedOhubID.unionByName,
      currentMerged,
      previousMerged,
      currentMergedOPR,
      config,
      spark
    )
  }
}

object OperatorOutboundWriter extends ExportOutboundWriter[Operator] with AcmOptions {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[Operator]) = {
    import spark.implicits._
    dataSet.map(OperatorAcmConverter.convert(_))
  }

  override def explainConversion: Option[Operator => AcmOperator] = Some((input: Operator) => OperatorAcmConverter.convert(input, true))

  override def entityName(): String = "OPERATORS"

  override def run(spark: SparkSession, config: OutboundConfig, storage: Storage): Unit = {
    import spark.implicits._

    log.info(
      s"For OP writing integrated entities ::   [${config.integratedInputFile}] " +
        s"with parameters currentMerged :: [${config.currentMerged}]" +
        s"with parameters previousMerged:: [${config.previousMerged}]" +
        s"with parameters prevIntegrated:: [${config.previousIntegratedInputFile}]" +
        s"with parameters currentMergedOPR:: [${config.currentMergedOPR}]" +
        s"to outbound export csv file for ACM and DDB.")

    val previousIntegratedFile = config.previousIntegratedInputFile.fold(spark.createDataset[Operator](Nil))(storage.readFromParquet[Operator](_))
    val currentIntegrated = storage.readFromParquet[Operator](config.integratedInputFile)
    val currentMerged = config.currentMerged.fold(spark.createDataset[Operator](Nil))(storage.readFromParquet[Operator](_))
    val previousMerged = config.previousMerged.fold(spark.createDataset[Operator](Seq()))(storage.readFromParquet[Operator](_))
    val deletedOhubID = getDeletedOhubIdsWithTargetId(spark, previousIntegratedFile, currentIntegrated, previousMerged, currentMerged)
    val currentMergedOPR = config.currentMergedOPR.fold(spark.emptyDataFrame)(spark.read.parquet(_))

    export(
      currentIntegrated,
      deletedOhubID.unionByName,
      currentMerged,
      previousMerged,
      currentMergedOPR,
      config,
      spark
    )
  }

}

object SubscriptionOutboundWriter extends ExportOutboundWriter[Subscription] with AcmOptions {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[Subscription]) = {
    import spark.implicits._
    dataSet.map(SubscriptionAcmConverter.convert(_))
  }

  override def explainConversion: Option[Subscription => AcmSubscription] = Some((input: Subscription) => SubscriptionAcmConverter.convert(input, true))

  override def entityName(): String = "SUBSCRIPTIONS"
}

object ProductOutboundWriter extends ExportOutboundWriter[Product] with AcmOptions {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[Product]) = {
    import spark.implicits._
    dataSet.map(ProductAcmConverter.convert(_))
  }

  override def explainConversion: Option[Product => AcmProduct] = Some((input: Product) => ProductAcmConverter.convert(input, true))

  override private[export] def entitySpecificFilter(spark: SparkSession, dataSet: Dataset[Product], config: OutboundConfig) = {
    import spark.implicits._
    dataSet.filter(!$"type".isNull || !$"sourceName".isin("DEX","FUZZIT"));
  }

  override def entityName(): String = "PRODUCTS"
}

object OrderOutboundWriter extends ExportOutboundWriter[Order] with AcmOptions {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[Order]) = {
    import spark.implicits._
    dataSet.map(OrderAcmConverter.convert(_))
  }

  override def explainConversion: Option[Order => AcmOrder] = Some((input: Order) => OrderAcmConverter.convert(input, true))


  override private[export] def entitySpecificFilter(spark: SparkSession, dataSet: Dataset[Order], config: OutboundConfig) = {
    import spark.implicits._
    dataSet.filter(!$"type".isin("SSD", "TRANSFER"));
  }

  override def entityName(): String = "ORDERS"
}

object OrderLineOutboundWriter extends ExportOutboundWriter[OrderLine] with AcmOptions {
  override private[export] def entitySpecificFilter(spark: SparkSession, dataSet: Dataset[OrderLine], config: OutboundConfig) = {
    dataSet.filter(o ⇒ {
      o.orderType.fold(true)(t => !(t.equals("SSD") || t.equals("TRANSFER")))
    })
  }

  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[OrderLine]) = {
    import spark.implicits._
    dataSet.map(OrderLineAcmConverter.convert(_))
  }

  override def explainConversion: Option[OrderLine => AcmOrderLine] = Some((input: OrderLine) => OrderLineAcmConverter.convert(input, true))

  override def entityName(): String = "ORDERLINES"
}

object ActivityOutboundWriter extends ExportOutboundWriter[Activity] with AcmOptions {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[Activity]) = {
    import spark.implicits._
    dataSet.map(ActivityAcmConverter.convert(_))
  }

  override def explainConversion: Option[Activity => AcmActivity] = Some((input: Activity) => ActivityAcmConverter.convert(input, true))

  override private[export] def entitySpecificFilter(spark: SparkSession, dataSet: Dataset[Activity], config: OutboundConfig) = {
    import spark.implicits._
    dataSet.filter($"customerType" === "CONTACTPERSON")
  }

  override def entityName(): String = "ACTIVITIES"
}

object LoyaltyPointsOutboundWriter extends ExportOutboundWriter[LoyaltyPoints] with AcmOptions {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[LoyaltyPoints]) = {
    import spark.implicits._
    dataSet.map(LoyaltyPointsAcmConverter.convert(_))
  }

  override def explainConversion: Option[LoyaltyPoints => AcmLoyaltyPoints] = Some((input: LoyaltyPoints) => LoyaltyPointsAcmConverter.convert(input, true))

  override def entityName(): String = "LOYALTIES"
}

/**
 * Runs concrete [[com.unilever.ohub.spark.export.ExportOutboundWriter]]'s run method for all
 * [[com.unilever.ohub.spark.domain.DomainEntity]]s acmExportWriter values.
 *
 * When running this job, do bear in mind that the input location is now a folder, the entity name will be appended to it
 * to determine the location.
 *
 * F.e. to export data from runId "2019-08-06" provide "integratedInputFile" as:
 * "dbfs:/mnt/engine/integrated/2019-08-06"
 * In this case CP will be fetched from:
 * "dbfs:/mnt/engine/integrated/2019-08-06/contactpersons.parquet"
 **/
object AllAcmOutboundWriter extends SparkJobWithOutboundExportConfig {
  override def run(spark: SparkSession, config: OutboundConfig, storage: Storage): Unit = {
    DomainEntityUtils.domainCompanionObjects
      .par
      .filter(_.acmExportWriter.isDefined)
      .foreach(entity => {
        val writer = entity.acmExportWriter.get
        val integratedLocation = s"${config.integratedInputFile}/${entity.engineFolderName}.parquet"
        val previousIntegratedLocation = config.previousIntegratedInputFile.map(prevIntegLocation => s"${config.previousIntegratedInputFile.get}/${entity.engineFolderName}.parquet")

        val currentMergedLocation = entity.engineGoldenFolderName.map(goldenFolderName => s"${config.integratedInputFile}/${entity.engineGoldenFolderName.get}.parquet")
        val previousMergedLocation = if (config.previousIntegratedInputFile.isDefined) {
          entity.engineGoldenFolderName.map(goldenFolderName => s"${config.previousIntegratedInputFile.get}/${entity.engineGoldenFolderName.get}.parquet")
        } else {
          None
        }

        val mappingOutputLocation1 = config.mappingOutputLocation.map(mappingOutboundLocation => s"${mappingOutboundLocation}/${config.targetType}_${writer.entityName()}_MAPPING.json")
        writer.run(
          spark,
          config.copy(
            integratedInputFile = integratedLocation,
            previousIntegratedInputFile = previousIntegratedLocation,
            currentMerged = currentMergedLocation,
            previousMerged = previousMergedLocation,
            mappingOutputLocation = mappingOutputLocation1),
          storage)
      })
  }
}
