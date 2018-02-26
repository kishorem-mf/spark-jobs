package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.data.OrderRecord
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.{Dataset, SparkSession}

case class OHubIdRefIdAndCountry(ohubId:String, refId:String, countryCode:String)

object OrderMergingOld extends App {

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
      val operatorRef = if (order.refOperatorId.isDefined) Some(s"${order.countryCode.get}~${order.source.get}~${order.refOperatorId.get}") else None
      val contactRef = if (order.refContactPersonId.isDefined) Some(s"${order.countryCode.get}~${order.source.get}~${order.refContactPersonId.get}") else None
      order.copy(refOperatorId = operatorRef, refContactPersonId = contactRef)
    })

  val operatorIdAndRefs:Dataset[OHubIdRefIdAndCountry] = spark.read.parquet(operatorMergingInputFile)
    .select($"ohubOperatorId", $"refIds", $"countryCode")
    .flatMap(row => row.getSeq[String](1).map(OHubIdRefIdAndCountry(row.getString(0),_, row.getString(2))))

  val contactPersonIdAndRefs:Dataset[OHubIdRefIdAndCountry] = spark.read.parquet(contactPersonMergingInputFile)
    .select($"ohubContactPersonId", $"refIds", $"countryCode")
    .flatMap(row => row.getSeq[String](1).map(OHubIdRefIdAndCountry(row.getString(0),_, row.getString(2))))

  val operatorsJoined:Dataset[OrderRecord] = orders
    .joinWith(operatorIdAndRefs, operatorIdAndRefs("countryCode") === orders("countryCode") and operatorIdAndRefs("refId") === orders("refOperatorId"), "left")
    .map(tuple => {
      val order:OrderRecord = tuple._1
      val operator:OHubIdRefIdAndCountry = Option(tuple._2).getOrElse(OHubIdRefIdAndCountry(s"REF_OPERATOR_UNKNOWN", "UNKNOWN", "UNKNOWN"))
      order.copy(refOperatorId = if (order.refOperatorId.isDefined) Some(operator.ohubId) else None)
    })

  val operatorsAndContactsJoined:Dataset[OrderRecord] = operatorsJoined
    .joinWith(contactPersonIdAndRefs, contactPersonIdAndRefs("countryCode") === operatorsJoined("countryCode") and contactPersonIdAndRefs("refId") === operatorsJoined("refContactPersonId"), "left")
    .map(tuple => {
      val order:OrderRecord = tuple._1
      val contact:OHubIdRefIdAndCountry = Option(tuple._2).getOrElse(OHubIdRefIdAndCountry(s"REF_CONTACT_PERSON_UNKNOWN", "UNKNOWN", "UNKNOWN"))
      order.copy(refContactPersonId = if (order.refContactPersonId.isDefined) Some(contact.ohubId) else None)
    })

  operatorsAndContactsJoined.write.mode(Overwrite).partitionBy("countryCode").format("parquet").save(outputFile)

  println(s"Done in ${(System.currentTimeMillis - startOfJob) / 1000}s")
}
