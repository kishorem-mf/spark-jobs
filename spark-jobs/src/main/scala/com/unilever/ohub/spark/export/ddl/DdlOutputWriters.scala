package com.unilever.ohub.spark.export.ddl

import com.unilever.ohub.spark.domain.DomainEntityUtils
import com.unilever.ohub.spark.domain.entity._
import com.unilever.ohub.spark.export._
import com.unilever.ohub.spark.export.ddl.model._
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{Dataset, SparkSession}

trait DdlOptions extends CsvOptions {

  override val delimiter: String = ";"

  override val extraOptions = Map(
    "delimiter" -> delimiter
  )

}

object OperatorDdlOutboundWriter extends ExportOutboundWriter[OperatorGolden] with DdlOptions {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[OperatorGolden]) = {
    import spark.implicits._
    dataSet.map(OperatorDdlConverter.convert(_))
  }

  override def explainConversion: Option[OperatorGolden => DdlOperator] = Some((input: OperatorGolden) => OperatorDdlConverter.convert(input, true))

  override def entityName(): String = "OPERATORS"

  override def run(spark: SparkSession, config: OutboundConfig, storage: Storage): Unit = {

    val currentIntegrated = storage.readFromParquet[OperatorGolden](config.integratedInputFile)
    val folderName = "operators"
    exportToDdl(
      currentIntegrated,
      folderName,
      config.copy(targetType = TargetType.DDL),
      spark
    )
  }
}

object ContactPersonDdlOutboundWriter extends ExportOutboundWriter[ContactPersonGolden] with DdlOptions {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[ContactPersonGolden]) = {
    import spark.implicits._
    dataSet.map(ContactPersonDdlConverter.convert(_))
  }

  override def explainConversion: Option[ContactPersonGolden => DdlContactPerson] = Some((input: ContactPersonGolden) => ContactPersonDdlConverter.convert(input, true))

  override def entityName(): String = "CONTACTPERSON"

  override def run(spark: SparkSession, config: OutboundConfig, storage: Storage): Unit = {

    val currentIntegrated = storage.readFromParquet[ContactPersonGolden](config.integratedInputFile)
    val folderName = "contacts"
    exportToDdl(
      currentIntegrated,
      folderName,
      config.copy(targetType = TargetType.DDL),
      spark
    )
  }

}

object OrderDdlOutboundWriter extends ExportOutboundWriter[Order] with DdlOptions {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[Order]) = {
    import spark.implicits._
    dataSet.map(OrderDdlConverter.convert(_))
  }

  override def explainConversion: Option[Order => DdlOrder] = Some((input: Order) => OrderDdlConverter.convert(input, true))

  override def entityName(): String = "ORDERS"

  override def run(spark: SparkSession, config: OutboundConfig, storage: Storage): Unit = {

    val currentIntegrated = storage.readFromParquet[Order](config.integratedInputFile)
    val folderName = "orders"

    exportToDdl(
      currentIntegrated,
      folderName,
      config.copy(targetType = TargetType.DDL),
      spark
    )
  }

}

object NewsletterSubscriptionDdlOutboundWriter extends ExportOutboundWriter[Subscription] with DdlOptions {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[Subscription]) = {
    import spark.implicits._
    dataSet.map(NewsletterSubscriptionDdlConverter.convert(_))
  }

  override def explainConversion: Option[Subscription => DdlNewsletterSubscription] = Some((input: Subscription)
  => NewsletterSubscriptionDdlConverter.convert(input, true))

  override def entityName(): String = "SUBSCRIPTION"

  override def run(spark: SparkSession, config: OutboundConfig, storage: Storage): Unit = {

    val currentIntegrated = storage.readFromParquet[Subscription](config.integratedInputFile)
    val folderName = "subscriptions"
    exportToDdl(
      currentIntegrated,
      folderName,
      config.copy(targetType = TargetType.DDL),
      spark
    )
  }

}

object OrderlineDdlOutboundWriter extends ExportOutboundWriter[OrderLine] with DdlOptions {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[OrderLine]) = {
    import spark.implicits._
    dataSet.map(OrderlineDdlConverter.convert(_))
  }

  override def explainConversion: Option[OrderLine => DdlOrderline] = Some((input: OrderLine)
  => OrderlineDdlConverter.convert(input, true))

  override def entityName(): String = "ORDERLINES"

  override def run(spark: SparkSession, config: OutboundConfig, storage: Storage): Unit = {

    val currentIntegrated = storage.readFromParquet[OrderLine](config.integratedInputFile)
    val folderName = "orderlines"
    exportToDdl(
      currentIntegrated,
      folderName,
      config.copy(targetType = TargetType.DDL),
      spark
    )
  }

}

object ProductDdlOutboundWriter extends ExportOutboundWriter[Product] with DdlOptions {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[Product]) = {
    import spark.implicits._
    dataSet.map(ProductsDdlConverter.convert(_))
  }

  override def explainConversion: Option[Product => DdlProducts] = Some((input: Product)
  => ProductsDdlConverter.convert(input, true))

  override def entityName(): String = "PRODUCTS"

  override def run(spark: SparkSession, config: OutboundConfig, storage: Storage): Unit = {

    val currentIntegrated = storage.readFromParquet[Product](config.integratedInputFile)
    val folderName = "products"
    exportToDdl(
      currentIntegrated,
      folderName,
      config.copy(targetType = TargetType.DDL),
      spark
    )
  }
}

object AssetDdlOutboundWriter extends ExportOutboundWriter[Asset] with DdlOptions {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[Asset]) = {
    import spark.implicits._
    dataSet.map(AssetDdlConverter.convert(_))
  }

  override def explainConversion: Option[Asset => DdlAssets] = Some((input: Asset)
  => AssetDdlConverter.convert(input, true))

  override def entityName(): String = "CABINET"

  override def run(spark: SparkSession, config: OutboundConfig, storage: Storage): Unit = {

    val currentIntegrated = storage.readFromParquet[Asset](config.integratedInputFile)
    val folderName = "cabinets"
    exportToDdl(
      currentIntegrated,
      folderName,
      config.copy(targetType = TargetType.DDL),
      spark
    )
  }

}

object AssetMovementDdlOutboundWriter extends ExportOutboundWriter[AssetMovement] with DdlOptions {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[AssetMovement]) = {
    import spark.implicits._
    dataSet.map(AssetMovementDdlConverter.convert(_))
  }

  override def explainConversion: Option[AssetMovement => DdlAssetMovements] = Some((input: AssetMovement)
  => AssetMovementDdlConverter.convert(input, true))

  override def entityName(): String = "CUSTOMER_CABINET"

  override def run(spark: SparkSession, config: OutboundConfig, storage: Storage): Unit = {

    val currentIntegrated = storage.readFromParquet[AssetMovement](config.integratedInputFile)
    val folderName = "customercabinets"
    exportToDdl(
      currentIntegrated,
      folderName,
      config.copy(targetType = TargetType.DDL),
      spark
    )
  }

}

object WholesalerAssignmentDdlOutboundWriter extends ExportOutboundWriter[WholesalerAssignment] with DdlOptions {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[WholesalerAssignment]) = {
    import spark.implicits._
    dataSet.map(WholesalerAssignmentDdlConverter.convert(_))
  }

  override def explainConversion: Option[WholesalerAssignment => DdlWholesalerAssignment] = Some((input: WholesalerAssignment)
  => WholesalerAssignmentDdlConverter.convert(input, true))

  override def entityName(): String = "WS_ASSIGNMENT"

  override def run(spark: SparkSession, config: OutboundConfig, storage: Storage): Unit = {

    val currentIntegrated = storage.readFromParquet[WholesalerAssignment](config.integratedInputFile)
    val folderName = "wsassignments"
    exportToDdl(
      currentIntegrated,
      folderName,
      config.copy(targetType = TargetType.DDL),
      spark
    )
  }

}

object AllDdlOutboundWriter extends SparkJobWithOutboundExportConfig {
  override def run(spark: SparkSession, config: OutboundConfig, storage: Storage): Unit = {
    DomainEntityUtils.domainCompanionObjects
      .par
      .filter(_.ddlExportWriter.isDefined)
      .foreach(entity => {
        val writer = entity.ddlExportWriter.get
        val integratedLocation = s"${config.integratedInputFile}/${entity.engineFolderName}.parquet"
        val mappingOutputLocation1 = config.mappingOutputLocation.map(mappingOutboundLocation => s"${mappingOutboundLocation}/${config.targetType}_${writer.entityName()}_MAPPING.json")
        writer.run(
          spark,
          config.copy(
            integratedInputFile = integratedLocation,
            outboundLocation = config.outboundLocation,
            auroraCountryCodes = config.auroraCountryCodes,
            fromDate = config.fromDate,
            toDate = config.toDate,
            sourceName = config.sourceName,
            mappingOutputLocation = mappingOutputLocation1),
          storage)
      })
  }
}
