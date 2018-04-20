package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.domain.entity.{ ContactPerson, Operator }
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }

// The step that fixes the foreign key links between contact persons and operators
// TODO Temporarily in a 2nd file to make development easier, will end up in the first ContactPersonMerging job eventually.
object ContactPersonMerging2 extends SparkJob {
  private case class OHubIdAndRefId(ohubId: String, refId: String)

  def transform(
    spark: SparkSession,
    contactPersonMatching: Dataset[ContactPerson],
    operatorIdAndRefs: Dataset[OHubIdAndRefId]
  ): Dataset[ContactPerson] = {
    import spark.implicits._

    contactPersonMatching
      .joinWith(
        operatorIdAndRefs,
        operatorIdAndRefs("refId") === contactPersonMatching("contactPerson.operatorConcatId"),
        JoinType.LeftOuter
      )
      .map {
        // TODO it's probably smarter to add another id to contact persons that refers to the operator ohub id, then both ref id's are present.
        case (contactPerson, maybeOperator) ⇒
          contactPerson.copy(operatorConcatId = Option(maybeOperator).map(_.ohubId).getOrElse("REF_OPERATOR_UNKNOWN"))
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

    val contactPersonMerging = storage.readFromParquet[ContactPerson](contactPersonMergingInputFile)

    val operatorIdAndRefs = storage
      .readFromParquet[Operator](operatorMergingInputFile)
      .map(op ⇒ OHubIdAndRefId(op.ohubId.get, op.concatId)) // TODO resolve .get?

    val transformed = transform(spark, contactPersonMerging, operatorIdAndRefs)

    storage.writeToParquet(transformed, outputFile, partitionBy = Seq("countryCode"))
  }
}
