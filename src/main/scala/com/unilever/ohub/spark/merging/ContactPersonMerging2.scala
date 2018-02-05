package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.generic.FileSystems
import org.apache.log4j.{ LogManager, Logger }
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.{ Dataset, SparkSession }

case class OHubIdAndRefId(OHUB_ID: String, REF_ID: String)

// The step that fixes the foreign key links between contact persons and operators
// Temporarily in a 2nd file to make development easier,
// will end up in the first ContactPersonMerging job eventually.
object ContactPersonMerging2 extends App {
  implicit private val log: Logger = LogManager.getLogger(this.getClass)

  val (contactPersonMatchingInputFile, operatorMatchingInputFile, outputFile) = FileSystems.getFileNames(
    args,
    "CONTACT_PERSON_MATCHING_INPUT_FILE", "OPERATOR_MATCHING_INPUT_FILE", "OUTPUT_FILE"
  )

  log.info(
    s"Merging contact persons from [$contactPersonMatchingInputFile] and [$operatorMatchingInputFile] " +
      s"to [$outputFile]"
  )

  val spark = SparkSession.
    builder().
    appName(this.getClass.getSimpleName).
    getOrCreate()

  import spark.implicits._

  // TODO run some performance tests on how this runs on a cluster VS forced repartition
  spark.sql("SET spark.sql.shuffle.partitions=20").collect()
  spark.sql("SET spark.default.parallelism=20").collect()

  val startOfJob = System.currentTimeMillis()

  val contactPersonMatching: Dataset[GoldenContactPersonRecord] = spark
    .read
    .parquet(contactPersonMatchingInputFile)
    .as[GoldenContactPersonRecord]
    .map(line => { // need the operator ref to have the data of a concat id
      val contact = line.CONTACT_PERSON
      line.copy(
        CONTACT_PERSON = contact.copy(
          REF_OPERATOR_ID = Some(
            s"${contact.COUNTRY_CODE.get}~${contact.SOURCE.get}~${contact.REF_OPERATOR_ID.get}"
          )
        )
      )
    })

  val operatorIdAndRefs:Dataset[OHubIdAndRefId] = spark.read.parquet(operatorMatchingInputFile)
    .select($"OHUB_OPERATOR_ID", $"REF_IDS")
    .flatMap(row => row.getSeq[String](1).map(OHubIdAndRefId(row.getString(0),_)))

  val joined:Dataset[GoldenContactPersonRecord] = contactPersonMatching
    .joinWith(
      operatorIdAndRefs,
      operatorIdAndRefs("REF_ID") === contactPersonMatching("CONTACT_PERSON.REF_OPERATOR_ID"),
      "left"
    )
    .map(tuple => {
      val contactPerson = tuple._1
      val operator = Option(tuple._2).getOrElse(OHubIdAndRefId(s"REF_OPERATOR_UNKNOWN", "UNKNOWN"))
      contactPerson.copy(
        CONTACT_PERSON = contactPerson.CONTACT_PERSON.copy(
          REF_OPERATOR_ID = Some(operator.OHUB_ID)
        )
      )
    })

  joined.write.mode(Overwrite).partitionBy("COUNTRY_CODE").format("parquet").save(outputFile)

  log.info(s"Done in ${(System.currentTimeMillis - startOfJob) / 1000}s")
}
