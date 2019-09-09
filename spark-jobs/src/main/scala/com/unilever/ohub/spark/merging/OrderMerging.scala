package com.unilever.ohub.spark.merging

import java.util.UUID

import com.unilever.ohub.spark.domain.entity.{ContactPerson, Operator, Order}
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{SparkJob, SparkJobConfig}
import org.apache.spark.sql.{Dataset, SparkSession}
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

    val mergedOrders = mergeOrders(spark, orders, previousIntegrated)

    mergedOrders
      .joinWith(operators, $"operatorConcatId" === operators("concatId"), "left")
      .map {
        case (order: Order, op: Operator) => order.copy(operatorOhubId = op.ohubId)
        case (order: Order, _) ⇒ order
      }
      .joinWith(contactPersons, $"contactPersonConcatId" === contactPersons("concatId"), "left")
      .map {
        case (order: Order, cpn: ContactPerson) => order.copy(contactPersonOhubId = cpn.ohubId)
        case (order, _) ⇒ order
      }
  }

  private def mergeOrders(spark: SparkSession,
                          orders: Dataset[Order],
                          previousIntegrated: Dataset[Order]) = {
    import spark.implicits._
    previousIntegrated
      .joinWith(orders, previousIntegrated("concatId") === orders("concatId"), JoinType.FullOuter)
      .map {
        case (integrated: Order, order: Order) ⇒ {
          val orderUid: Option[String] = order.orderUid.orElse(integrated.orderUid).orElse(Some(UUID.randomUUID().toString))
          order.copy(ohubId = integrated.ohubId, orderUid = orderUid, isGoldenRecord = true)
        }
        case (integrated: Order, _) => integrated
        case (_, order: Order) => order.copy(
          ohubId = Some(UUID.randomUUID().toString),
          orderUid = order.orderUid.orElse(Some(UUID.randomUUID().toString)),
          isGoldenRecord = true
        )
      }
  }

  override private[spark] def defaultConfig = OrderMergingConfig()

  override private[spark] def configParser(): OptionParser[OrderMergingConfig] =
    new scopt.OptionParser[OrderMergingConfig]("Order merging") {
      head("merges orders into an integrated order output file.", "1.0")
      opt[String]("contactPersonInputFile") required() action { (x, c) ⇒
        c.copy(contactPersonInputFile = x)
      } text "contactPersonInputFile is a string property"
      opt[String]("operatorInputFile") required() action { (x, c) ⇒
        c.copy(operatorInputFile = x)
      } text "operatorInputFile is a string property"
      opt[String]("orderInputFile") required() action { (x, c) ⇒
        c.copy(orderInputFile = x)
      } text "orderInputFile is a string property"
      opt[String]("previousIntegrated") optional() action { (x, c) ⇒
        c.copy(previousIntegrated = x)
      } text "previousIntegrated is a string property"
      opt[String]("outputFile") required() action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"

      version("1.0")
      help("help") text "help text"
    }

  override def run(spark: SparkSession, config: OrderMergingConfig, storage: Storage): Unit = {
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
