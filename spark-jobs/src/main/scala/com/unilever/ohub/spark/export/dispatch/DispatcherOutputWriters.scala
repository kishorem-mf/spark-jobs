package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.domain.DomainEntityUtils
import com.unilever.ohub.spark.domain.entity._
import com.unilever.ohub.spark.domain.utils.OperatorRef
import com.unilever.ohub.spark.export.dispatch.model.{DispatchContactPerson, _}
import com.unilever.ohub.spark.export.{CsvOptions, ExportOutboundWriter, OutboundConfig, OutboundEntity, SparkJobWithOutboundExportConfig}
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

trait DispatcherOptions extends CsvOptions {

  override val delimiter: String = ";"

  override val extraOptions = Map(
    "delimiter" -> delimiter
  )

  override val mustQuotesFields: Boolean = true
}


object ContactPersonOutboundWriter extends ExportOutboundWriter[ContactPerson] with DispatcherOptions {

  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[ContactPerson]) = {
    import spark.implicits._
    dataSet.map(ContactPersonDispatchConverter.convert(_))
  }

  override def explainConversion: Option[ContactPerson => DispatchContactPerson] = Some((input: ContactPerson) => ContactPersonDispatchConverter.convert(input, true))

  override def entityName(): String = "CONTACT_PERSONS"

  override def linkOperator[GenericOutboundEntity <: OutboundEntity](spark: SparkSession, operatorDS: Dataset[_], deltaDs: Dataset[_]): Dataset[_] = {
    import spark.implicits._
    deltaDs.joinWith(operatorDS.select("ohubId", "concatId")
      .toDF("opr_ohub", "opr_concatId").as[OperatorRef], col("operatorOhubId") === col("opr_ohub"), "left").map {
      case (cp: ContactPerson, opRef: OperatorRef) if cp.isGoldenRecord => cp.copy(operatorConcatId = Option(opRef.opr_concatId), sourceName = "OHUB",
        sourceEntityId = cp.ohubId.getOrElse(cp.sourceEntityId))
      case (cp: ContactPerson, _) if cp.isGoldenRecord => cp.copy(sourceName = "OHUB", sourceEntityId = cp.ohubId.getOrElse(cp.sourceEntityId))
      case (cp: ContactPerson, _) => cp
    }
  }

  override def run(spark: SparkSession, config: OutboundConfig, storage: Storage): Unit = {
    import spark.implicits._

    log.info(
      s" For CP writing integrated entities :::   [${config.integratedInputFile}] " +
        s"with parameters currentMerged ::: [${config.currentMerged}]" +
        s"with parameters previousMerged::: [${config.previousMerged}]" +
        s"with parameters prevIntegrated::: [${config.previousIntegratedInputFile}]" +
        s"with parameters currentMergedOPR::: [${config.currentMergedOPR}]" +
        s"to outbound export csv file for ACM and DDB.")

    val previousIntegratedFile = config.previousIntegratedInputFile.fold(spark.createDataset[ContactPerson](Nil))(storage.readFromParquet[ContactPerson](_))
    val currentIntegrated = storage.readFromParquet[ContactPerson](config.integratedInputFile)
    val deltaIntegrated = commonTransform(currentIntegrated, previousIntegratedFile, config, spark).map(_.copy(isGoldenRecord = false))
    val currentMerged = config.currentMerged.fold(spark.createDataset[ContactPerson](Nil))(storage.readFromParquet[ContactPerson](_))
    val previousMerged = config.previousMerged.fold(spark.createDataset[ContactPerson](Nil))(storage.readFromParquet[ContactPerson](_))
    val deletedOhubID = getDeletedOhubIdsWithTargetIdDBB(spark, previousIntegratedFile, currentIntegrated, previousMerged, currentMerged)
    val currentMergedOPR = config.currentMergedOPR.fold(spark.emptyDataFrame)(spark.read.parquet(_))


    export(
      currentIntegrated,
      deltaIntegrated.unionByName(deletedOhubID).unionByName,
      currentMerged,
      previousMerged,
      currentMergedOPR,
      config,
      spark
    )
  }
}

object OperatorOutboundWriter extends ExportOutboundWriter[Operator] with DispatcherOptions {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[Operator]) = {
    import spark.implicits._
    dataSet.map(OperatorDispatchConverter.convert(_))
  }

  override def explainConversion: Option[Operator => DispatchOperator] = Some((input: Operator) => OperatorDispatchConverter.convert(input, true))

  override def entityName(): String = "OPERATORS"

  override def linkOperator[GenericOutboundEntity <: OutboundEntity](spark: SparkSession, operatorDS: Dataset[_], deltaDs: Dataset[_]): Dataset[_] = {
    import spark.implicits._
    deltaDs.map {
      case op: Operator if op.isGoldenRecord => op.copy(sourceName = "OHUB", sourceEntityId = op.ohubId.getOrElse(op.sourceEntityId))
      case op: Operator => op
    }
  }

  override def run(spark: SparkSession, config: OutboundConfig, storage: Storage): Unit = {
    import spark.implicits._

    log.info(
      s"For OP writing integrated entities :::   [${config.integratedInputFile}] " +
        s"with parameters currentMerged ::: [${config.currentMerged}]" +
        s"with parameters previousMerged::: [${config.previousMerged}]" +
        s"with parameters prevIntegrated::: [${config.previousIntegratedInputFile}]" +
        s"with parameters currentMergedOPR::: [${config.currentMergedOPR}]" +
        s"to outbound export csv file for ACM and DDB.")

    val previousIntegratedFile = config.previousIntegratedInputFile.fold(spark.createDataset[Operator](Nil))(storage.readFromParquet[Operator](_))
    val currentIntegrated = storage.readFromParquet[Operator](config.integratedInputFile)
    val deltaIntegrated = commonTransform(currentIntegrated, previousIntegratedFile, config, spark).map(_.copy(isGoldenRecord = false))
    val currentMerged = config.currentMerged.fold(spark.createDataset[Operator](Nil))(storage.readFromParquet[Operator](_))
    val previousMerged = config.previousMerged.fold(spark.createDataset[Operator](Nil))(storage.readFromParquet[Operator](_))
    val deletedOhubID = getDeletedOhubIdsWithTargetIdDBB(spark, previousIntegratedFile, currentIntegrated, previousMerged, currentMerged)
    val currentMergedOPR = config.currentMergedOPR.fold(spark.emptyDataFrame)(spark.read.parquet(_))

    export(
      currentIntegrated,
      deltaIntegrated.unionByName(deletedOhubID).unionByName,
      currentMerged,
      previousMerged,
      currentMergedOPR,
      config,
      spark
    )
  }

}

object SubscriptionOutboundWriter extends ExportOutboundWriter[Subscription] with DispatcherOptions {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[Subscription]) = {
    import spark.implicits._
    dataSet.map(SubscriptionDispatchConverter.convert(_))
  }

  override def explainConversion: Option[Subscription => DispatchSubscription] = Some((input: Subscription) => SubscriptionDispatchConverter.convert(input, true))

  override def entityName(): String = "SUBSCRIPTIONS"
}

object ProductOutboundWriter extends ExportOutboundWriter[Product] with DispatcherOptions {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[Product]) = {
    import spark.implicits._
    dataSet.map(ProductDispatchConverter.convert(_))
  }

  override def explainConversion: Option[Product => DispatchProduct] = Some((input: Product) => ProductDispatchConverter.convert(input, true))

  override def entityName(): String = "ORDER_PRODUCTS"
}

object OrderOutboundWriter extends ExportOutboundWriter[Order] with DispatcherOptions {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[Order]) = {
    import spark.implicits._
    dataSet.map(OrderDispatchConverter.convert(_))
  }

  override def explainConversion: Option[Order => DispatchOrder] = Some((input: Order) => OrderDispatchConverter.convert(input, true))

  override private[export] def entitySpecificFilter(spark: SparkSession, dataSet: Dataset[Order], config: OutboundConfig) = {
    import spark.implicits._
    dataSet.filter(!$"type".isin("SSD", "TRANSFER"));
  }

  override def entityName(): String = "ORDERS"
}

object OrderLineOutboundWriter extends ExportOutboundWriter[OrderLine] with DispatcherOptions {
  override private[spark] def entitySpecificFilter(spark: SparkSession, dataSet: Dataset[OrderLine], config: OutboundConfig) = {
    dataSet.filter(o ⇒ {
      o.orderType match {
        case Some(t) ⇒ !(t.equals("SSD") || t.equals("TRANSFER"))
        case None ⇒ true
      }
    })
  }

  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[OrderLine]) = {
    import spark.implicits._
    dataSet.map(OrderLineDispatchConverter.convert(_))
  }

  override def explainConversion: Option[OrderLine => DispatchOrderLine] = Some((input: OrderLine) => OrderLineDispatchConverter.convert(input, true))

  override def entityName(): String = "ORDER_LINES"
}

object ActivityOutboundWriter extends ExportOutboundWriter[Activity] with DispatcherOptions {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[Activity]) = {
    import spark.implicits._
    dataSet.map(ActivityDispatcherConverter.convert(_))
  }

  override def explainConversion: Option[Activity => DispatchActivity] = Some((input: Activity) => ActivityDispatcherConverter.convert(input, true))

  override private[export] def entitySpecificFilter(spark: SparkSession, dataSet: Dataset[Activity], config: OutboundConfig) = {
    import spark.implicits._
    dataSet.filter($"customerType" === "CONTACTPERSON")
  }

  override def entityName(): String = "CONTACT_PERSON_ACTIVITIES"
}

object LoyaltyPointsOutboundWriter extends ExportOutboundWriter[LoyaltyPoints] with DispatcherOptions {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[LoyaltyPoints]) = {
    import spark.implicits._
    dataSet.map(LoyaltyPointsDispatcherConverter.convert(_))
  }

  override def explainConversion: Option[LoyaltyPoints => DispatchLoyaltyPoints] = Some((input: LoyaltyPoints) => LoyaltyPointsDispatcherConverter.convert(input, true))

  override private[export] def entitySpecificFilter(spark: SparkSession, dataSet: Dataset[LoyaltyPoints], config: OutboundConfig) = {
    import spark.implicits._
    dataSet.filter($"isGoldenRecord")
  }

  override def entityName(): String = "LOYALTIES"
}

object CampaignOutboundWriter extends ExportOutboundWriter[Campaign] with DispatcherOptions {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[Campaign]) = {
    import spark.implicits._

    dataSet.map(CampaignDispatcherConverter.convert(_))
  }

  override def explainConversion: Option[Campaign => DispatchCampaign] = Some((input: Campaign) => CampaignDispatcherConverter.convert(input, true))

  override def entityName(): String = "CAMPAIGNS"
}

object CampaignBounceOutboundWriter extends ExportOutboundWriter[CampaignBounce] with DispatcherOptions {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[CampaignBounce]) = {
    import spark.implicits._

    dataSet.map(CampaignBounceDispatcherConverter.convert(_))
  }

  override def explainConversion: Option[CampaignBounce => DispatchCampaignBounce] = Some((input: CampaignBounce) => CampaignBounceDispatcherConverter.convert(input, true))

  override def entityName(): String = "CW_BOUNCES"
}

object CampaignClickOutboundWriter extends ExportOutboundWriter[CampaignClick] with DispatcherOptions {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[CampaignClick]) = {
    import spark.implicits._

    dataSet.map(CampaignClickDispatcherConverter.convert(_))
  }

  override def explainConversion: Option[CampaignClick => DispatchCampaignClick] = Some((input: CampaignClick) => CampaignClickDispatcherConverter.convert(input, true))

  override def entityName(): String = "CW_CLICKS"
}

object CampaignOpenOutboundWriter extends ExportOutboundWriter[CampaignOpen] with DispatcherOptions {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[CampaignOpen]) = {
    import spark.implicits._

    dataSet.map(CampaignOpenDispatcherConverter.convert(_))
  }

  override def explainConversion: Option[CampaignOpen => DispatchCampaignOpen] = Some((input: CampaignOpen) => CampaignOpenDispatcherConverter.convert(input, true))

  override def entityName(): String = "CW_OPENS"
}

object CampaignSendOutboundWriter extends ExportOutboundWriter[CampaignSend] with DispatcherOptions {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[CampaignSend]) = {
    import spark.implicits._

    dataSet.map(CampaignSendDispatcherConverter.convert(_))
  }

  override def explainConversion: Option[CampaignSend => DispatchCampaignSend] = Some((input: CampaignSend) => CampaignSendDispatcherConverter.convert(input, true))

  override def entityName(): String = "CW_SENDINGS"
}

object ChainOutboundWriter extends ExportOutboundWriter[Chain] with DispatcherOptions {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[Chain]) = {
    import spark.implicits._

    dataSet.map(ChainDispatchConverter.convert(_))
  }

  override def explainConversion: Option[Chain => DispatchChain] = Some((input: Chain) => ChainDispatchConverter.convert(input, true))

  override def entityName(): String = "CHAINS"
}

/**
 * Runs concrete [[com.unilever.ohub.spark.export.ExportOutboundWriter]]'s run method for all
 * [[com.unilever.ohub.spark.domain.DomainEntity]]s dispatchExportWriter values.
 *
 * When running this job, do bear in mind that the input location is now a folder, the entity name will be appended to it
 * to determine the location.
 *
 * F.e. to export data from runId "2019-08-06" provide "integratedInputFile" as:
 * "dbfs:/mnt/engine/integrated/2019-08-06"
 * In this case CP will be fetched from:
 * "dbfs:/mnt/engine/integrated/2019-08-06/contactpersons.parquet"
 **/
object AllDispatchOutboundWriter extends SparkJobWithOutboundExportConfig {
  override def run(spark: SparkSession, config: OutboundConfig, storage: Storage): Unit = {
    DomainEntityUtils.domainCompanionObjects
      .par
      .filter(_.dispatchExportWriter.isDefined)
      .foreach(entity => {
        val writer = entity.dispatchExportWriter.get
        val integratedLocation = s"${config.integratedInputFile}/${entity.engineFolderName}.parquet"

        val mappingOutputLocation = config.mappingOutputLocation.map(mappingOutput => s"$mappingOutput/${config.targetType}_${writer.entityName()}_MAPPING.json")

        val previousIntegratedLocation = config.previousIntegratedInputFile.map(prevIntegLocation => s"$prevIntegLocation/${entity.engineFolderName}.parquet")

        val currentMergedLocation = entity.engineGoldenFolderName.map(goldenFolderName => s"${config.integratedInputFile}/${goldenFolderName}.parquet")
        val previousMergedLocation = config.previousIntegratedInputFile.flatMap { previousIntegratedLocation =>
          entity.engineGoldenFolderName.map(goldenFolderName => s"$previousIntegratedLocation/$goldenFolderName.parquet")
        }


        val currentMergedOPRLocation = entity.engineGoldenFolderName.find(_ == "contactpersons_golden").map(_ => s"${config.integratedInputFile}/operators_golden.parquet")

        writer.run(
          spark,
          config.copy(
            integratedInputFile = integratedLocation,
            previousIntegratedInputFile = previousIntegratedLocation,
            currentMerged = currentMergedLocation,
            previousMerged = previousMergedLocation,
            mappingOutputLocation = mappingOutputLocation,
            currentMergedOPR = currentMergedOPRLocation),
          storage)
      })
  }
}
