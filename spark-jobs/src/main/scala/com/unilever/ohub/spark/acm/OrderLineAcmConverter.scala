package com.unilever.ohub.spark.acm

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.acm.model.AcmOrderLine
import com.unilever.ohub.spark.domain.entity.OrderLine
import com.unilever.ohub.spark.export.DeltaFunctions
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

object OrderLineAcmConverter extends SparkJob[DefaultWithDeltaConfig]
  with DeltaFunctions with AcmTransformationFunctions with AcmConverter {

  def transform(
    spark: SparkSession,
    orderLines: Dataset[OrderLine],
    previousIntegrated: Dataset[OrderLine]
  ): Dataset[AcmOrderLine] = {
    val dailyAcmOrderLines = createAcmOrderLines(spark, orderLines)
    val allPreviousAcmOrderLines = createAcmOrderLines(spark, previousIntegrated)

    integrate[AcmOrderLine](spark, dailyAcmOrderLines, allPreviousAcmOrderLines, "ORDERLINE_ID")
  }

  override private[spark] def defaultConfig = DefaultWithDeltaConfig()

  override private[spark] def configParser(): OptionParser[DefaultWithDeltaConfig] = DefaultWithDeltaConfigParser()

  override def run(spark: SparkSession, config: DefaultWithDeltaConfig, storage: Storage): Unit = {
    import spark.implicits._

    log.info(s"Generating order lines ACM csv file from [${config.inputFile}] to [${config.outputFile}]")

    val orderLines = storage.readFromParquet[OrderLine](config.inputFile)
    val previousIntegrated = config.previousIntegrated match {
      case Some(s) ⇒ storage.readFromParquet[OrderLine](s)
      case None ⇒
        log.warn(s"No existing integrated file specified -- regarding as initial load.")
        spark.emptyDataset[OrderLine]
    }

    val transformed = transform(spark, orderLines, previousIntegrated)

    storage.writeToSingleCsv(transformed, config.outputFile, extraWriteOptions)
  }

  def createAcmOrderLines(spark: SparkSession, orderLines: Dataset[OrderLine]): Dataset[AcmOrderLine] = {
    import spark.implicits._

    orderLines.map(orderLine ⇒ AcmOrderLine(
      ORDERLINE_ID = orderLine.concatId,
      ORD_INTEGRATION_ID = orderLine.orderConcatId,
      QUANTITY = orderLine.quantityOfUnits,
      AMOUNT = orderLine.amount,
      LOYALTY_POINTS = orderLine.loyaltyPoints,
      PRD_INTEGRATION_ID = orderLine.productConcatId,
      SAMPLE_ID = "",
      CAMPAIGN_LABEL = orderLine.campaignLabel,
      COMMENTS = orderLine.comment,
      DELETED_FLAG = boolAsString(!orderLine.isActive)
    ))
  }

}
