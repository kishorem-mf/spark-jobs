package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.generic.FileSystems
import com.unilever.ohub.spark.tsv2parquet.OrderRecord
import org.apache.log4j.{ LogManager, Logger }
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.SparkSession

case class OHubIdRefIdAndCountry(OHUB_ID: String, COUNTRY_CODE: String, REF_ID: Option[String] = None)

// Technically not really order MERGING, but we need to update foreign key IDs in the other records
object OrderMerging extends App {
  implicit private val log: Logger = LogManager.getLogger(this.getClass)

  val (
    contactPersonMergingInputFile: String,
    operatorMergingInputFile: String,
    orderInputFile: String,
    outputFile: String
  ) = FileSystems.getFileNames(
      args,
      "CONTACT_PERSON_MERGING_INPUT_FILE", "OPERATOR_MERGING_INPUT_FILE", "ORDER_INPUT_FILE", "OUTPUT_FILE"
    )

  log.info(
    s"Merging orders from [$contactPersonMergingInputFile], [$operatorMergingInputFile] " +
      s"and [$orderInputFile] to [$outputFile]"
  )

  val spark = SparkSession.
    builder().
    appName(this.getClass.getSimpleName).
    getOrCreate()

  import spark.implicits._

  val startOfJob = System.currentTimeMillis()

  val orders = spark
    .read
    .parquet(orderInputFile)
    .as[OrderRecord]
    .map(order => {
      val operatorRef = order.REF_OPERATOR_ID.map { refOperatorId =>
        s"${order.COUNTRY_CODE.get}~${order.SOURCE.get}~$refOperatorId"
      }
      val contactRef = order.REF_CONTACT_PERSON_ID.map { refContactPersonId =>
        s"${order.COUNTRY_CODE.get}~${order.SOURCE.get}~$refContactPersonId"
      }
      order.copy(REF_OPERATOR_ID = operatorRef, REF_CONTACT_PERSON_ID = contactRef)
    })

  val operatorIdAndRefs = spark.read.parquet(operatorMergingInputFile)
    .select($"OHUB_OPERATOR_ID", $"REF_IDS", $"COUNTRY_CODE")
    .flatMap(row => row.getSeq[String](1).map(_ => OHubIdRefIdAndCountry(row.getString(0), row.getString(2))))

  val contactPersonIdAndRefs = spark.read.parquet(contactPersonMergingInputFile)
    .select($"OHUB_CONTACT_PERSON_ID", $"REF_IDS", $"COUNTRY_CODE")
    .flatMap(row => row.getSeq[String](1).map(_ => OHubIdRefIdAndCountry(row.getString(0), row.getString(2))))

  val defaultOperator = OHubIdRefIdAndCountry(s"REF_OPERATOR_UNKNOWN", "UNKNOWN", Some("UNKNOWN"))
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

  val defaultContact = OHubIdRefIdAndCountry(s"REF_CONTACT_PERSON_UNKNOWN", "UNKNOWN", Some("UNKNOWN"))
  val operatorsAndContactsJoined = operatorsJoined
  .joinWith(
    contactPersonIdAndRefs,
    contactPersonIdAndRefs("COUNTRY_CODE") === operatorsJoined("COUNTRY_CODE")
      and contactPersonIdAndRefs("REF_ID") === operatorsJoined("REF_CONTACT_PERSON_ID"),
    "left"
  )
  .map {
    case (order, ctct) =>
      val contact = Option(ctct).getOrElse(defaultContact)
      order.copy(REF_CONTACT_PERSON_ID = order.REF_CONTACT_PERSON_ID.map(_ => contact.OHUB_ID))
  }

  operatorsAndContactsJoined
    .write
    .mode(Overwrite)
    .partitionBy("COUNTRY_CODE")
    .format("parquet")
    .save(outputFile)

  log.info(s"Done in ${(System.currentTimeMillis - startOfJob) / 1000}s")
}
