package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.data.{ GoldenContactPersonRecord, GoldenOperatorRecord }
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }

// The step that fixes the foreign key links between contact persons and operators
// Temporarily in a 2nd file to make development easier,
// will end up in the first ContactPersonMerging job eventually.
object ContactPersonMerging2 extends SparkJob {
  def transform(
    spark: SparkSession,
    contactPersonMatching: Dataset[GoldenContactPersonRecord],
    operatorIdAndRefs: Dataset[GoldenOperatorRecord]
  ): Dataset[GoldenContactPersonRecord] = {
    import spark.implicits._

    contactPersonMatching
      .joinWith(
        operatorIdAndRefs,
        operatorIdAndRefs("refId") === contactPersonMatching("contactPerson.refOperatorId"),
        JoinType.LeftOuter
      )
      .map {
        case (contactPerson, maybeOperator) => Option(maybeOperator).fold(contactPerson) { operator =>
          contactPerson.copy(
            contactPerson = contactPerson.contactPerson.copy(
              refOperatorId = Option(operator.ohubOperatorId)
            )
          )
        }
      }
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
      .readFromParquet[GoldenOperatorRecord](operatorMergingInputFile)

    val transformed = transform(spark, contactPersonMerging, operatorIdAndRefs)

    storage
      .writeToParquet(transformed, outputFile, partitionBy = "countryCode")
  }
}
