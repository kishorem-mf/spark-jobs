package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import com.unilever.ohub.spark.domain.entity.OrderLine
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.sql.JoinType
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

case class OrderLineMergingConfig(
    orderLineInputFile: String = "order-input-file",
    previousIntegrated: Option[String] = None,
    outputFile: String = "path-to-output-file"
) extends SparkJobConfig

// Technically not really order MERGING, but we need to update foreign key IDs in the other records
object OrderLineMerging extends SparkJob[OrderLineMergingConfig] {

  def transform(
    spark: SparkSession,
    orders: Dataset[OrderLine],
    previousIntegrated: Dataset[OrderLine]
  ): Dataset[OrderLine] = {
    import spark.implicits._

    previousIntegrated
      .joinWith(orders, previousIntegrated("concatId") === orders("concatId"), JoinType.FullOuter)
      .map {
        case (integrated, orderLine) ⇒
          if (orderLine == null) {
            integrated
          } else if (integrated == null) {
            orderLine
          } else {
            orderLine.copy(ohubId = integrated.ohubId, ohubCreated = integrated.ohubCreated)
          }
      }
  }

  override private[spark] def defaultConfig = OrderLineMergingConfig()

  override private[spark] def configParser(): OptionParser[OrderLineMergingConfig] =
    new scopt.OptionParser[OrderLineMergingConfig]("Order merging") {
      head("merges orders into an integrated order output file.", "1.0")
      opt[String]("orderLineInputFile") required () action { (x, c) ⇒
        c.copy(orderLineInputFile = x)
      } text "orderLineInputFile is a string property"
      opt[String]("previousIntegrated") optional () action { (x, c) ⇒
        c.copy(previousIntegrated = Some(x))
      } text "previousIntegrated is a string property"
      opt[String]("outputFile") required () action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"

      version("1.0")
      help("help") text "help text"
    }

  override def run(spark: SparkSession, config: OrderLineMergingConfig, storage: Storage): Unit = {
    import spark.implicits._

    val orderRecords = storage.readFromParquet[OrderLine](config.orderLineInputFile)
    val previousIntegrated = config.previousIntegrated match {
      case Some(s) ⇒ storage.readFromParquet[OrderLine](s)
      case None ⇒
        log.warn(s"No existing integrated file specified -- regarding as initial load.")
        spark.emptyDataset[OrderLine]
    }

    val transformed = transform(spark, orderRecords, previousIntegrated)

    storage.writeToParquet(transformed, config.outputFile)
  }
}
