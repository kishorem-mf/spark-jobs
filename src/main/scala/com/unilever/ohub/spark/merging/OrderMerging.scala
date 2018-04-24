package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import com.unilever.ohub.spark.data.OrderRecord
import com.unilever.ohub.spark.domain.entity.{ ContactPerson, Operator }
import com.unilever.ohub.spark.generic.StringFunctions
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.sql.JoinType
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

case class OrderMergingConfig(
    contactPersonInputFile: String = "contact-person-input-file",
    operatorInputFile: String = "operator-input-file",
    orderInputFile: String = "order-input-file",
    outputFile: String = "path-to-output-file"
) extends SparkJobConfig

// Technically not really order MERGING, but we need to update foreign key IDs in the other records
object OrderMerging extends SparkJob[OrderMergingConfig] {
  private case class OHubIdRefIdAndCountry(ohubId: String, refId: String, countryCode: Option[String])

  def transform(
    spark: SparkSession,
    orderRecords: Dataset[OrderRecord],
    operatorRecords: Dataset[Operator],
    contactPersonRecords: Dataset[ContactPerson]
  ): Dataset[OrderRecord] = {
    import spark.implicits._

    val orders = orderRecords
      .map(order ⇒ {
        val operatorRef = order.refOperatorId.map { refOperatorId ⇒
          StringFunctions.createConcatId(order.countryCode, order.source, refOperatorId)
        }
        val contactRef = order.refContactPersonId.map { refContactPersonId ⇒
          StringFunctions.createConcatId(order.countryCode, order.source, refContactPersonId)
        }
        order.copy(
          refOperatorId = operatorRef,
          refContactPersonId = contactRef
        )
      })

    val operators = operatorRecords
      .map { operator ⇒
        OHubIdRefIdAndCountry(operator.ohubId.get, operator.concatId, Some(operator.countryCode))
      }

    val contactPersons = contactPersonRecords
      .map { contactPerson ⇒
        OHubIdRefIdAndCountry(contactPerson.ohubId.get, contactPerson.concatId, Some(contactPerson.countryCode))
      }

    val operatorsJoined = orders
      .joinWith(
        operators,
        operators("countryCode") === orders("countryCode")
          and operators("refId") === orders("refOperatorId"),
        JoinType.Left
      )
      .map {
        case (order, maybeOperator) ⇒
          val refOperatorId = Option(maybeOperator).map(_.ohubId).getOrElse("REF_OPERATOR_UNKNOWN")
          order.copy(refOperatorId = Some(refOperatorId))
      }

    operatorsJoined
      .joinWith(
        contactPersons,
        contactPersons("countryCode") === operatorsJoined("countryCode")
          and contactPersons("refId") === operatorsJoined("refContactPersonId"),
        JoinType.Left
      )
      .map {
        case (order, maybeContactPerson) ⇒
          val refContactPersonId = Option(maybeContactPerson)
            .map(_.ohubId)
            .getOrElse("REF_CONTACT_PERSON_UNKNOWN")
          order.copy(refContactPersonId = Some(refContactPersonId))
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

    val orderRecords = storage.readFromParquet[OrderRecord](config.orderInputFile)
    val operatorRecords = storage.readFromParquet[Operator](config.operatorInputFile)
    val contactPersonRecords = storage.readFromParquet[ContactPerson](config.contactPersonInputFile)
    val transformed = transform(spark, orderRecords, operatorRecords, contactPersonRecords)

    storage.writeToParquet(transformed, config.outputFile, partitionBy = Seq("countryCode"))
  }
}
