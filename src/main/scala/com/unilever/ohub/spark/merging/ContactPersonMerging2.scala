package com.unilever.ohub.spark.merging

import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.{Dataset, SparkSession}

case class OHubIdAndRefId(OHUB_OPERATOR_ID:String, REF_ID:String)

// The step that fixes the foreign key links between contact persons and operators
// Temporarily in a 2nd file to make development easier, will end up in the first ContactPersonMerging job eventually.
object ContactPersonMerging2 extends App {

  if (args.length != 3) {
    println("specify CONTACT_PERSON_MATCHING_INPUT_FILE OPERATOR_MATCHING_INPUT_FILE OUTPUT_FILE")
    sys.exit(1)
  }

  val contactPersonMatchingInputFile = args(0)
  val operatorMatchingInputFile = args(1)
  val outputFile = args(2)

  println(s"Merging contact persons from [$contactPersonMatchingInputFile] and [$operatorMatchingInputFile] to [$outputFile]")

  val spark = SparkSession.
    builder().
    appName(this.getClass.getSimpleName).
    getOrCreate()

  import spark.implicits._

  // TODO run some performance tests on how this runs on a cluster VS forced repartition
  spark.sql("SET spark.sql.shuffle.partitions=20").collect()
  spark.sql("SET spark.default.parallelism=20").collect()

  val startOfJob = System.currentTimeMillis()

  val contactPersonMatching: Dataset[GoldenContactPersonRecord] = spark.read.parquet(contactPersonMatchingInputFile)
    .as[GoldenContactPersonRecord]
    .map(line => { // need the operator ref to have the data of a concat id
      val contact = line.CONTACT_PERSON
      line.copy(CONTACT_PERSON = contact.copy(REF_OPERATOR_ID = Some(s"${contact.COUNTRY_CODE.get}~${contact.SOURCE.get}~${contact.REF_OPERATOR_ID.get}")))
    })

  val operatorIdAndRefs:Dataset[OHubIdAndRefId] = spark.read.parquet(operatorMatchingInputFile)
    .select($"OHUB_OPERATOR_ID", $"REF_IDS")
    .flatMap(row => row.getSeq[String](1).map(OHubIdAndRefId(row.getString(0),_)))

  val joined:Dataset[GoldenContactPersonRecord] = contactPersonMatching
    .joinWith(operatorIdAndRefs, operatorIdAndRefs("REF_ID") === contactPersonMatching("CONTACT_PERSON.REF_OPERATOR_ID"), "left")
    .map(tuple => {
      val contactPerson:GoldenContactPersonRecord = tuple._1
      val operator:OHubIdAndRefId = Option(tuple._2).getOrElse(OHubIdAndRefId(s"REF_OPERATOR_UNKNOWN", "UNKNOWN"))
      contactPerson.copy(CONTACT_PERSON = contactPerson.CONTACT_PERSON.copy(REF_OPERATOR_ID = Some(operator.OHUB_OPERATOR_ID)))
    })

  joined.write.mode(Overwrite).partitionBy("COUNTRY_CODE").format("parquet").save(outputFile)

  println(s"Done in ${(System.currentTimeMillis - startOfJob) / 1000}s")
}
