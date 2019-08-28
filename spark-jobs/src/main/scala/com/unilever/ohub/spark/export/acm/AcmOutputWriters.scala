package com.unilever.ohub.spark.export.acm

import com.unilever.ohub.spark.domain.DomainEntityUtils
import com.unilever.ohub.spark.domain.entity._
import com.unilever.ohub.spark.export.{CsvOptions, ExportOutboundWriter, OutboundConfig, SparkJobWithOutboundExportConfig}
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{Dataset, SparkSession}

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

  override def explainConversion = Some((input: ContactPerson) => ContactPersonAcmConverter.convert(input, true))

  override def entityName(): String = "RECIPIENTS"
}

object OperatorOutboundWriter extends ExportOutboundWriter[Operator] with AcmOptions {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[Operator]) = {
    import spark.implicits._
    dataSet.map(OperatorAcmConverter.convert(_))
  }

  override def explainConversion = Some((input: Operator) => OperatorAcmConverter.convert(input, true))

  override def entityName(): String = "OPERATORS"
}

object SubscriptionOutboundWriter extends ExportOutboundWriter[Subscription] with AcmOptions {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[Subscription]) = {
    import spark.implicits._
    dataSet.map(SubscriptionAcmConverter.convert(_))
  }

  override def explainConversion = Some((input: Subscription) => SubscriptionAcmConverter.convert(input, true))

  override def entityName(): String = "SUBSCRIPTIONS"
}

object ProductOutboundWriter extends ExportOutboundWriter[Product] with AcmOptions {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[Product]) = {
    import spark.implicits._
    dataSet.map(ProductAcmConverter.convert(_))
  }

  override def explainConversion = Some((input: Product) => ProductAcmConverter.convert(input, true))

  override def entityName(): String = "PRODUCTS"
}

object OrderOutboundWriter extends ExportOutboundWriter[Order] with AcmOptions {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[Order]) = {
    import spark.implicits._
    dataSet.map(OrderAcmConverter.convert(_))
  }

  override def explainConversion = Some((input: Order) => OrderAcmConverter.convert(input, true))


  override private[export] def filterDataSet(spark: SparkSession, dataSet: Dataset[Order], config: OutboundConfig) = {
    import spark.implicits._
    dataSet.filter(!$"type".isin("SSD", "TRANSFER"));
  }

  override def entityName(): String = "ORDERS"
}

object OrderLineOutboundWriter extends ExportOutboundWriter[OrderLine] with AcmOptions {
  override private[export] def filterDataSet(spark: SparkSession, dataSet: Dataset[OrderLine], config: OutboundConfig) = {
    dataSet.filter(o ⇒ {
      o.orderType match {
        case Some(t) ⇒ !(t.equals("SSD") || t.equals("TRANSFER"))
        case None ⇒ true
      }
    })
  }

  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[OrderLine]) = {
    import spark.implicits._
    dataSet.map(OrderLineAcmConverter.convert(_))
  }

  override def explainConversion = Some((input: OrderLine) => OrderLineAcmConverter.convert(input, true))

  override def entityName(): String = "ORDERLINES"
}

object ActivityOutboundWriter extends ExportOutboundWriter[Activity] with AcmOptions {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[Activity]) = {
    import spark.implicits._
    dataSet.map(ActivityAcmConverter.convert(_))
  }

  override def explainConversion = Some((input: Activity) => ActivityAcmConverter.convert(input, true))

  override private[export] def filterDataSet(spark: SparkSession, dataSet: Dataset[Activity], config: OutboundConfig) = {
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

  override def explainConversion = Some((input: LoyaltyPoints) => LoyaltyPointsAcmConverter.convert(input, true))

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
        val hashLocation = config.hashesInputFile.map(x => x + s"/${entity.engineFolderName}.parquet")
        val mappingOutputLocation = config.mappingOutputLocation.map(mappingOutboundLocation => s"${mappingOutboundLocation}/${config.targetType}_${writer.entityName()}_MAPPING.json")
        writer.run(
          spark,
          config.copy(
            integratedInputFile = integratedLocation,
            hashesInputFile = hashLocation,
            mappingOutputLocation = mappingOutputLocation),
          storage)
      })
  }
}
