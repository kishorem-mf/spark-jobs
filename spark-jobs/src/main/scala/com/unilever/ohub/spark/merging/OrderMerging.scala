package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import com.unilever.ohub.spark.domain.entity.{ Order, ContactPerson, Operator }
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.sql.JoinType
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

case class OrderMergingConfig(
    contactPersonInputFile: String = "contact-person-input-file",
    operatorInputFile: String = "operator-input-file",
    previousIntegrated: String = "previous-integrated-orders",
    orderInputFile: String = "order-input-file",
    outputFile: String = "path-to-output-file"
) extends SparkJobConfig

// Technically not really order MERGING, but we need to update foreign key IDs in the other records
object OrderMerging extends SparkJob[OrderMergingConfig] {

  def transform(
    spark: SparkSession,
    orders: Dataset[Order],
    previousIntegrated: Dataset[Order],
    operators: Dataset[Operator],
    contactPersons: Dataset[ContactPerson]
  ): Dataset[Order] = {
    import spark.implicits._

    val allOrders =
      previousIntegrated
        .joinWith(orders, previousIntegrated("concatId") === orders("concatId"), JoinType.FullOuter)
        .map {
          case (integrated, order) ⇒
            if (order == null) {
              integrated
            } else if (integrated == null) {
              order
            } else {
              order.copy(ohubId = integrated.ohubId)
            }
        }

    allOrders
      .joinWith(operators, $"operatorConcatId" === operators("concatId"), "left")
      .map {
        case (order, op) ⇒
          if (op == null) order
          else order.copy(operatorOhubId = op.ohubId)
      }
      .joinWith(contactPersons, $"contactPersonConcatId" === contactPersons("concatId"), "left")
      .map {
        case (order, cpn) ⇒
          if (cpn == null) order
          else order.copy(contactPersonOhubId = cpn.ohubId)
      }
  }

  override private[spark] def defaultConfig = OrderMergingConfig()

  override private[spark] def configParser(): OptionParser[OrderMergingConfig] =
    new scopt.OptionParser[OrderMergingConfig]("Order merging") {
      head("merges orders into an integrated order output file.", "1.0")
      opt[String]("contactPersonInputFile") required () action { (x, c) ⇒
        c.copy(contactPersonInputFile = x)
      } text "contactPersonInputFile is a string property"
      opt[String]("operatorInputFile") required () action { (x, c) ⇒
        c.copy(operatorInputFile = x)
      } text "operatorInputFile is a string property"
      opt[String]("orderInputFile") required () action { (x, c) ⇒
        c.copy(orderInputFile = x)
      } text "orderInputFile is a string property"
      opt[String]("previousIntegrated") optional () action { (x, c) ⇒
        c.copy(previousIntegrated = x)
      } text "previousIntegrated is a string property"
      opt[String]("outputFile") required () action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"

      version("1.0")
      help("help") text "help text"
    }

  override def run(spark: SparkSession, config: OrderMergingConfig, storage: Storage): Unit = {
    import spark.implicits._

    log.info(
      s"Merging orders from [${config.contactPersonInputFile}], [${config.operatorInputFile}] " +
        s"and [${config.orderInputFile}] to [${config.outputFile}]"
    )

    val orderRecords = storage.readFromParquet[Order](config.orderInputFile)
    val operatorRecords = storage.readFromParquet[Operator](config.operatorInputFile)
    val contactPersonRecords = storage.readFromParquet[ContactPerson](config.contactPersonInputFile)
    val previousIntegrated = storage.readFromParquet[Order](config.previousIntegrated)

    val transformed = transform(spark, orderRecords, previousIntegrated, operatorRecords, contactPersonRecords)

    storage.writeToParquet(transformed, config.outputFile)
  }
}
