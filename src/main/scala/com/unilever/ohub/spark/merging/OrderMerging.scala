package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.tsv2parquet.OrderRecord
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.{Dataset, SparkSession}

case class OHubIdRefIdAndCountry(OHUB_ID:String, REF_ID:String, COUNTRY_CODE:String)

// Technically not really order MERGING, but we need to update foreign key IDs in the other records
object OrderMerging extends App {

  if (args.length != 4) {
    println("specify CONTACT_PERSON_MERGING_INPUT_FILE OPERATOR_MERGING_INPUT_FILE ORDER_INPUT_FILE OUTPUT_FILE")
    sys.exit(1)
  }

  val contactPersonMergingInputFile = args(0)
  val operatorMergingInputFile = args(1)
  val orderInputFile = args(2)
  val outputFile = args(3)

  println(s"Merging orders from [$contactPersonMergingInputFile], [$operatorMergingInputFile] and [$orderInputFile] to [$outputFile]")

  val spark = SparkSession.
    builder().
    appName(this.getClass.getSimpleName).
    getOrCreate()

  import spark.implicits._

  val startOfJob = System.currentTimeMillis()

  val orders:Dataset[OrderRecord] = spark.read.parquet(orderInputFile)
    .as[OrderRecord]
    .map(order => {
      val operatorRef = if (order.REF_OPERATOR_ID.isDefined) Some(s"${order.COUNTRY_CODE.get}~${order.SOURCE.get}~${order.REF_OPERATOR_ID.get}") else None
      val contactRef = if (order.REF_CONTACT_PERSON_ID.isDefined) Some(s"${order.COUNTRY_CODE.get}~${order.SOURCE.get}~${order.REF_CONTACT_PERSON_ID.get}") else None
      order.copy(REF_OPERATOR_ID = operatorRef, REF_CONTACT_PERSON_ID = contactRef)
    })

  val operatorIdAndRefs:Dataset[OHubIdRefIdAndCountry] = spark.read.parquet(operatorMergingInputFile)
    .select($"OHUB_OPERATOR_ID", $"REF_IDS", $"COUNTRY_CODE")
    .flatMap(row => row.getSeq[String](1).map(OHubIdRefIdAndCountry(row.getString(0),_, row.getString(2))))

  val contactPersonIdAndRefs:Dataset[OHubIdRefIdAndCountry] = spark.read.parquet(contactPersonMergingInputFile)
    .select($"OHUB_CONTACT_PERSON_ID", $"REF_IDS", $"COUNTRY_CODE")
    .flatMap(row => row.getSeq[String](1).map(OHubIdRefIdAndCountry(row.getString(0),_, row.getString(2))))

  val operatorsJoined:Dataset[OrderRecord] = orders
    .joinWith(operatorIdAndRefs, operatorIdAndRefs("COUNTRY_CODE") === orders("COUNTRY_CODE") and operatorIdAndRefs("REF_ID") === orders("REF_OPERATOR_ID"), "left")
    .map(tuple => {
      val order:OrderRecord = tuple._1
      val operator:OHubIdRefIdAndCountry = Option(tuple._2).getOrElse(OHubIdRefIdAndCountry(s"REF_OPERATOR_UNKNOWN", "UNKNOWN", "UNKNOWN"))
      order.copy(REF_OPERATOR_ID = if (order.REF_OPERATOR_ID.isDefined) Some(operator.OHUB_ID) else None)
    })

  val operatorsAndContactsJoined:Dataset[OrderRecord] = operatorsJoined
  .joinWith(contactPersonIdAndRefs, contactPersonIdAndRefs("COUNTRY_CODE") === operatorsJoined("COUNTRY_CODE") and contactPersonIdAndRefs("REF_ID") === operatorsJoined("REF_CONTACT_PERSON_ID"), "left")
  .map(tuple => {
    val order:OrderRecord = tuple._1
    val contact:OHubIdRefIdAndCountry = Option(tuple._2).getOrElse(OHubIdRefIdAndCountry(s"REF_CONTACT_PERSON_UNKNOWN", "UNKNOWN", "UNKNOWN"))
    order.copy(REF_CONTACT_PERSON_ID = if (order.REF_CONTACT_PERSON_ID.isDefined) Some(contact.OHUB_ID) else None)
  })

  operatorsAndContactsJoined.write.mode(Overwrite).partitionBy("COUNTRY_CODE").format("parquet").save(outputFile)

  println(s"Done in ${(System.currentTimeMillis - startOfJob) / 1000}s")
}
