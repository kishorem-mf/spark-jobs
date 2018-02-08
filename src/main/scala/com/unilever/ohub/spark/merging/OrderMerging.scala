package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.sql.Left
import com.unilever.ohub.spark.tsv2parquet.OrderRecord
import org.apache.spark.sql.{ Dataset, SparkSession }

case class OHubIdRefIdAndCountry(
  OHUB_ID: String,
  COUNTRY_CODE: String,
  REF_ID: Option[String] = None
)

case class OHubOperatorIdRefIdsAndCountry(
  OHUB_OPERATOR_ID: String,
  COUNTRY_CODE: String,
  REF_IDS: Seq[String]
)

case class OHubContactPersonIdRefIdsAndCountry(
  OHUB_CONTACT_PERSON_ID: String,
  COUNTRY_CODE: String,
  REF_IDS: Seq[String]
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
        val operatorRef = order.REF_OPERATOR_ID.map { refOperatorId =>
          s"${order.COUNTRY_CODE.get}~${order.SOURCE.get}~$refOperatorId"
        }
        val contactRef = order.REF_CONTACT_PERSON_ID.map { refContactPersonId =>
          s"${order.COUNTRY_CODE.get}~${order.SOURCE.get}~$refContactPersonId"
        }
        order.copy(REF_OPERATOR_ID = operatorRef, REF_CONTACT_PERSON_ID = contactRef)
      })

    val operatorsJoined = orders
      .joinWith(
        operatorIdAndRefs,
        operatorIdAndRefs("COUNTRY_CODE") === orders("COUNTRY_CODE")
          and operatorIdAndRefs("REF_ID") === orders("REF_OPERATOR_ID"),
        "left"
      )
      .map {
        case (order, opr) =>
          val operator = Option(opr).getOrElse(defaultOperator)
          order.copy(REF_OPERATOR_ID = order.REF_OPERATOR_ID.map(_ => operator.OHUB_ID))
      }

    operatorsJoined
      .joinWith(
        contactPersonIdAndRefs,
        contactPersonIdAndRefs("COUNTRY_CODE") === operatorsJoined("COUNTRY_CODE")
          and contactPersonIdAndRefs("REF_ID") === operatorsJoined("REF_CONTACT_PERSON_ID"),
        Left
      )
      .map {
        case (order, oHubIdRefIdAndCountry) =>
          val contact = Option(oHubIdRefIdAndCountry).getOrElse(defaultContact)
          order.copy(REF_CONTACT_PERSON_ID = order.REF_CONTACT_PERSON_ID.map(_ => contact.OHUB_ID))
      }
  }

  override val neededFilePaths = Array(
    "CONTACT_PERSON_MERGING_INPUT_FILE", "OPERATOR_MERGING_INPUT_FILE", "ORDER_INPUT_FILE", "OUTPUT_FILE"
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
        selectColumns = $"OHUB_OPERATOR_ID", $"REF_IDS", $"COUNTRY_CODE"
      )
      .flatMap { oHubOperatorIdRefIdsAndCountry =>
        oHubOperatorIdRefIdsAndCountry.REF_IDS.map { refId =>
          OHubIdRefIdAndCountry(refId, oHubOperatorIdRefIdsAndCountry.COUNTRY_CODE)
        }
      }

    val contactPersonIdAndRefs = storage
      .readFromParquet[OHubContactPersonIdRefIdsAndCountry](
        contactPersonMergingInputFile,
        selectColumns = $"OHUB_CONTACT_PERSON_ID", $"REF_IDS", $"COUNTRY_CODE"
      )
      .flatMap { oHubContactPersonIdRefIdsAndCountry =>
        oHubContactPersonIdRefIdsAndCountry.REF_IDS.map { refId =>
          OHubIdRefIdAndCountry(refId, oHubContactPersonIdRefIdsAndCountry.COUNTRY_CODE)
        }
      }

    val transformed = transform(spark, orders, operatorIdAndRefs, contactPersonIdAndRefs)

    storage
      .writeToParquet(transformed, outputFile, partitionBy = "COUNTRY_CODE")
  }
}
