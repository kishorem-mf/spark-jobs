package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.data.OrderRecord
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.sql.JoinType
import org.apache.spark.sql.{ Dataset, SparkSession }

case class OHubIdRefIdAndCountry(
  ohubId: String,
  countryCode: String,
  refId: Option[String] = None
)

case class OHubOperatorIdRefIdsAndCountry(
  ohubOperatorId: String,
  countryCode: String,
  refIds: Seq[String]
)

case class OHubContactPersonIdRefIdsAndCountry(
  ohubContactPersonId: String,
  countryCode: String,
  refIds: Seq[String]
)

// Technically not really order MERGING, but we need to update foreign key IDs in the other records
object OrderMerging extends SparkJob {
  private val defaultContact = OHubIdRefIdAndCountry("REF_CONTACT_PERSON_UNKNOWN", "UNKNOWN", Some("UNKNOWN"))
  private val defaultOperator = OHubIdRefIdAndCountry(s"REF_OPERATOR_UNKNOWN", "UNKNOWN", Some("UNKNOWN"))

  def transform(
    spark: SparkSession,
    orderRecords: Dataset[OrderRecord],
    operatorIdAndRefs: Dataset[OHubIdRefIdAndCountry],
    contactPersonIdAndRefs: Dataset[OHubIdRefIdAndCountry]
  ): Dataset[OrderRecord] = {
    import spark.implicits._

    val orders = orderRecords
      .map(order => {
        val operatorRef = order.refOperatorId.map { refOperatorId =>
          s"${order.countryCode.getOrElse("")}~${order.source.getOrElse("")}~$refOperatorId"
        }
        val contactRef = order.refContactPersonId.map { refContactPersonId =>
          s"${order.countryCode.getOrElse("")}~${order.source.getOrElse("")}~$refContactPersonId"
        }
        order.copy(
          refOperatorId = operatorRef,
          refContactPersonId = contactRef
        )
      })

    val operatorsJoined = orders
      .joinWith(
        operatorIdAndRefs,
        operatorIdAndRefs("countryCode") === orders("countryCode")
          and operatorIdAndRefs("REF_ID") === orders("REF_OPERATOR_ID"),
        JoinType.Left
      )
      .map {
        case (order, opr) =>
          val operator = Option(opr).getOrElse(defaultOperator)
          order.copy(refOperatorId = order.refOperatorId.map(_ => operator.ohubId))
      }

    operatorsJoined
      .joinWith(
        contactPersonIdAndRefs,
        contactPersonIdAndRefs("countryCode") === operatorsJoined("countryCode")
          and contactPersonIdAndRefs("REF_ID") === operatorsJoined("REF_CONTACT_PERSON_ID"),
        JoinType.Left
      )
      .map {
        case (order, oHubIdRefIdAndCountry) =>
          val contact = Option(oHubIdRefIdAndCountry).getOrElse(defaultContact)
          order.copy(refContactPersonId = order.refContactPersonId.map(_ => contact.ohubId))
      }
  }

  override val neededFilePaths = Array(
    "CONTACT_PERSON_MERGING_INPUT_FILE",
    "OPERATOR_MERGING_INPUT_FILE",
    "ORDER_INPUT_FILE",
    "OUTPUT_FILE"
  )

  override def run(spark: SparkSession, filePaths: Product, storage: Storage): Unit = {
    import spark.implicits._

    val (
      contactPersonMergingInputFile: String,
      operatorMergingInputFile: String,
      orderInputFile: String,
      outputFile: String
    ) = filePaths

    log.info(
      s"Merging orders from [$contactPersonMergingInputFile], [$operatorMergingInputFile] " +
        s"and [$orderInputFile] to [$outputFile]"
    )

    val orders = storage
      .readFromParquet[OrderRecord](orderInputFile)

    val operatorIdAndRefs = storage
      .readFromParquet[OHubOperatorIdRefIdsAndCountry](
        operatorMergingInputFile,
        selectColumns = $"ohubOperatorId", $"refIds", $"countryCode"
      )
      .flatMap { oHubOperatorIdRefIdsAndCountry =>
        oHubOperatorIdRefIdsAndCountry.refIds.map { refId =>
          OHubIdRefIdAndCountry(refId, oHubOperatorIdRefIdsAndCountry.countryCode)
        }
      }

    val contactPersonIdAndRefs = storage
      .readFromParquet[OHubContactPersonIdRefIdsAndCountry](
        contactPersonMergingInputFile,
        selectColumns = $"ohubContactPersonId", $"refIds", $"countryCode"
      )
      .flatMap { oHubContactPersonIdRefIdsAndCountry =>
        oHubContactPersonIdRefIdsAndCountry.refIds.map { refId =>
          OHubIdRefIdAndCountry(refId, oHubContactPersonIdRefIdsAndCountry.countryCode)
        }
      }

    val transformed = transform(spark, orders, operatorIdAndRefs, contactPersonIdAndRefs)

    storage
      .writeToParquet(transformed, outputFile, partitionBy = "countryCode")
  }
}
