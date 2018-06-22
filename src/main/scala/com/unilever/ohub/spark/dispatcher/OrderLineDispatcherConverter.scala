package com.unilever.ohub.spark.dispatcher

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.dispatcher.model.DispatcherOrderLine
import com.unilever.ohub.spark.domain.entity.OrderLine
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

object OrderLineDispatcherConverter extends SparkJob[DefaultWithDbAndDeltaConfig]
  with DeltaFunctions {

  def transform(
    spark: SparkSession,
    orderLines: Dataset[OrderLine],
    previousIntegrated: Dataset[OrderLine]
  ): Dataset[DispatcherOrderLine] = {
    val dailyOrderLines = createDispatcherOrderLines(spark, orderLines)
    val allPreviousOrderLines = createDispatcherOrderLines(spark, previousIntegrated)

    integrate[DispatcherOrderLine](spark, dailyOrderLines, allPreviousOrderLines, "ODL_INTEGRATION_ID")
  }

  override private[spark] def defaultConfig = DefaultWithDbAndDeltaConfig()

  override private[spark] def configParser(): OptionParser[DefaultWithDbAndDeltaConfig] = DefaultWithDbAndDeltaConfigParser()

  override def run(spark: SparkSession, config: DefaultWithDbAndDeltaConfig, storage: Storage): Unit = {
    import spark.implicits._

    log.info(s"Generating order lines dispatcher csv file from [${config.inputFile}] to [${config.outputFile}]")

    val orderLines = storage.readFromParquet[OrderLine](config.inputFile)
    val previousIntegrated = config.previousIntegrated match {
      case Some(s) ⇒ storage.readFromParquet[OrderLine](s)
      case None ⇒
        log.warn(s"No existing integrated file specified -- regarding as initial load.")
        spark.emptyDataset[OrderLine]
    }

    val transformed = transform(spark, orderLines, previousIntegrated)

    storage.writeToSingleCsv(transformed, config.outputFile, EXTRA_WRITE_OPTIONS)
  }

  def createDispatcherOrderLines(spark: SparkSession, orderLines: Dataset[OrderLine]): Dataset[DispatcherOrderLine] = {
    import spark.implicits._
    orderLines.map(DispatcherOrderLine.fromOrderLine)
  }
}
