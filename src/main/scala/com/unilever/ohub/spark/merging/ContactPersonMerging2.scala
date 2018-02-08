package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.sql.LeftOuter
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }

case class OHubOperatorIdAndRefIds(OHUB_OPERATOR_ID: String, REF_IDS: Seq[String])

case class OHubIdAndRefId(OHUB_ID: String, REF_ID: String)

// The step that fixes the foreign key links between contact persons and operators
// Temporarily in a 2nd file to make development easier,
// will end up in the first ContactPersonMerging job eventually.
object ContactPersonMerging2 extends SparkJob {
  private val defaultOperator = OHubIdAndRefId(s"REF_OPERATOR_UNKNOWN", "UNKNOWN")

  def addRefOperatorIdToContactPerson(
    contactPersonAndOHubIdAndRefId: (GoldenContactPersonRecord, OHubIdAndRefId)
  ): GoldenContactPersonRecord = {
    val (contactPersonRecord, oHubIdAndRefId) = contactPersonAndOHubIdAndRefId

    val operator = Option(oHubIdAndRefId).getOrElse(defaultOperator)

    contactPersonRecord.copy(
      CONTACT_PERSON = contactPersonRecord.CONTACT_PERSON.copy(
        REF_OPERATOR_ID = Some(operator.OHUB_ID)
      )
    )
  }

  def transform(
    spark: SparkSession,
    contactPersonMatching: Dataset[GoldenContactPersonRecord],
    operatorIdAndRefs: Dataset[OHubIdAndRefId]
  ): Dataset[GoldenContactPersonRecord] = {
    import spark.implicits._

    contactPersonMatching
      .joinWith(
        operatorIdAndRefs,
        operatorIdAndRefs("REF_ID") === contactPersonMatching("CONTACT_PERSON.REF_OPERATOR_ID"),
        LeftOuter
      )
      .map(addRefOperatorIdToContactPerson)
  }

  override val neededFilePaths = Array(
    "CONTACT_PERSON_MATCHING_INPUT_FILE",
    "OPERATOR_MATCHING_INPUT_FILE",
    "OUTPUT_FILE"
  )

  override def run(spark: SparkSession, filePaths: Product, storage: Storage): Unit = {
    import spark.implicits._

    val (contactPersonMatchingInputFile: String, operatorMatchingInputFile: String, outputFile: String) =
      filePaths

    // TODO run some performance tests on how this runs on a cluster VS forced repartition
    spark.sql("SET spark.sql.shuffle.partitions=20").collect()
    spark.sql("SET spark.default.parallelism=20").collect()

    val contactPersonMatching = storage
      .readFromParquet[GoldenContactPersonRecord](contactPersonMatchingInputFile)
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

    val operatorIdAndRefs = storage
      .readFromParquet[OHubOperatorIdAndRefIds](
        operatorMatchingInputFile,
        selectColumns = $"OHUB_OPERATOR_ID", $"REF_IDS"
      )
      .flatMap(foo => foo.REF_IDS.map(OHubIdAndRefId(foo.OHUB_OPERATOR_ID, _)))

    val transformed = transform(spark, contactPersonMatching, operatorIdAndRefs)

    storage
      .writeToParquet(transformed, outputFile, partitionBy = "COUNTRY_CODE")
  }
}
