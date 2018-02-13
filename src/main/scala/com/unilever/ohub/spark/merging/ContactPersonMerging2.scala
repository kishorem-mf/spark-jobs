package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.data.GoldenContactPersonRecord
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }

case class OHubOperatorIdAndRefIds(ohubOperatorId: String, refIds: Seq[String])

case class OHubIdAndRefId(ohubId: String, refId: String)

// The step that fixes the foreign key links between contact persons and operators
// Temporarily in a 2nd file to make development easier,
// will end up in the first ContactPersonMerging job eventually.
object ContactPersonMerging2 extends SparkJob {
  private val defaultOperator = OHubIdAndRefId("REF_OPERATOR_UNKNOWN", "UNKNOWN")

  def addRefOperatorIdToContactPerson(
    contactPersonAndOHubIdAndRefId: (GoldenContactPersonRecord, OHubIdAndRefId)
  ): GoldenContactPersonRecord = {
    val (contactPersonRecord, oHubIdAndRefId) = contactPersonAndOHubIdAndRefId

    val operator = Option(oHubIdAndRefId).getOrElse(defaultOperator)

    contactPersonRecord.copy(
      contactPerson = contactPersonRecord.contactPerson.copy(
        refOperatorId = Some(operator.ohubId)
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
        operatorIdAndRefs("refId") === contactPersonMatching("contactPerson.refOperatorId"),
        JoinType.LeftOuter
      )
      .map(addRefOperatorIdToContactPerson)
  }

  override val neededFilePaths = Array(
    "CONTACT_PERSON_MERGING_INPUT_FILE",
    "OPERATOR_MERGING_INPUT_FILE",
    "OUTPUT_FILE"
  )

  override def run(spark: SparkSession, filePaths: Product, storage: Storage): Unit = {
    import spark.implicits._

    val (contactPersonMergingInputFile: String, operatorMergingInputFile: String, outputFile: String) =
      filePaths

    log.info(
      s"Merging contact persons from [$contactPersonMergingInputFile] " +
        s"and [$operatorMergingInputFile] " +
        s"to [$outputFile]"
    )

    // TODO run some performance tests on how this runs on a cluster VS forced repartition
    spark.sql("SET spark.sql.shuffle.partitions=20").collect()
    spark.sql("SET spark.default.parallelism=20").collect()

    val contactPersonMerging = storage
      .readFromParquet[GoldenContactPersonRecord](contactPersonMergingInputFile)
      .map(line => { // need the operator ref to have the data of a concat id
        val contact = line.contactPerson
        line.copy(
          contactPerson = contact.copy(
            refOperatorId = Some(
              s"${contact.countryCode.getOrElse("")}~" +
                s"${contact.source.getOrElse("")}~" +
                s"${contact.refOperatorId.getOrElse("")}"
            )
          )
        )
      })

    val operatorIdAndRefs = storage
      .readFromParquet[OHubOperatorIdAndRefIds](
        operatorMergingInputFile,
        selectColumns = $"ohubOperatorId", $"refIds"
      )
      .flatMap { oHubOperatorIdAndRefIds =>
        oHubOperatorIdAndRefIds.refIds.map(OHubIdAndRefId(oHubOperatorIdAndRefIds.ohubOperatorId, _))
      }

    val transformed = transform(spark, contactPersonMerging, operatorIdAndRefs)

    storage
      .writeToParquet(transformed, outputFile, partitionBy = "countryCode")
  }
}
