package com.unilever.ohub.spark.acm

import java.util.UUID

import com.unilever.ohub.spark.{ SparkJob, DefaultConfig, SparkJobWithDefaultConfig }
import com.unilever.ohub.spark.acm.model.UFSOrderLine
import com.unilever.ohub.spark.domain.entity.OrderLine
import com.unilever.ohub.spark.generic.StringFunctions
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

object OrderLineAcmConverter extends SparkJob[DefaultWithDbAndDeltaConfig]
  with DeltaFunctions with AcmTransformationFunctions with AcmConverter {

  def transform(
    spark: SparkSession,
    orderLines: Dataset[OrderLine],
    previousIntegrated: Dataset[OrderLine]
  ): Dataset[UFSOrderLine] = {
    val dailyUfsOrderLines = createUfsOrderLines(spark, orderLines)
    val allPreviousUfsOrderLines = createUfsOrderLines(spark, previousIntegrated)

    integrate[UFSOrderLine](spark, dailyUfsOrderLines, allPreviousUfsOrderLines, "ORDERLINE_ID")
  }

  override private[spark] def defaultConfig = DefaultWithDbAndDeltaConfig()

  override private[spark] def configParser(): OptionParser[DefaultWithDbAndDeltaConfig] = DefaultWithDbAndDeltaConfigParser()

  override def run(spark: SparkSession, config: DefaultWithDbAndDeltaConfig, storage: Storage): Unit = {
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

    storage.writeToSingleCsv(transformed, config.outputFile, delim = outputCsvDelimiter, quote = outputCsvQuote)(log)
  }

  def createUfsOrderLines(spark: SparkSession, orderLines: Dataset[OrderLine]): Dataset[UFSOrderLine] = {
    import spark.implicits._

    orderLines.map(orderLine ⇒ UFSOrderLine(
      ORDER_ID = orderLine.concatId,
      ORDERLINE_ID = UUID.randomUUID().toString,
      PRD_INTEGRATION_ID = orderLine.productConcatId,
      QUANTITY = orderLine.quantityOfUnits,
      AMOUNT = orderLine.amount.map(_.setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble),
      SAMPLE_ID = ""
    ))
  }

}
