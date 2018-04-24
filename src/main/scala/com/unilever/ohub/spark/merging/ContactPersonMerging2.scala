package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import com.unilever.ohub.spark.domain.entity.{ ContactPerson, Operator }
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

case class ContactPersonMergingConfig(
    matchingInputFile: String = "matching-input-file",
    operatorInputFile: String = "operator-input-file",
    outputFile: String = "path-to-output-file"
) extends SparkJobConfig

// The step that fixes the foreign key links between contact persons and operators
// TODO Temporarily in a 2nd file to make development easier, will end up in the first ContactPersonMerging job eventually.
object ContactPersonMerging2 extends SparkJob[ContactPersonMergingConfig] {
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

  override private[spark] def defaultConfig = ContactPersonMergingConfig()

  override private[spark] def configParser(): OptionParser[ContactPersonMergingConfig] =
    new scopt.OptionParser[ContactPersonMergingConfig]("Domain gate keeper") {
      head("converts a csv into domain entities and writes the result to parquet.", "1.0")
      opt[String]("matchingInputFile") required () action { (x, c) ⇒
        c.copy(matchingInputFile = x)
      } text "matchingInputFile is a string property"
      opt[String]("operatorInputFile") required () action { (x, c) ⇒
        c.copy(operatorInputFile = x)
      } text "operatorInputFile is a string property"
      opt[String]("outputFile") required () action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"

      version("1.0")
      help("help") text "help text"
    }

  override def run(spark: SparkSession, config: ContactPersonMergingConfig, storage: Storage): Unit = {
    import spark.implicits._

    log.info(
      s"Merging contact persons from [${config.matchingInputFile}] " +
        s"and [${config.operatorInputFile}] " +
        s"to [${config.outputFile}]"
    )

    // TODO run some performance tests on how this runs on a cluster VS forced repartition
    spark.sql("SET spark.sql.shuffle.partitions=20").collect()
    spark.sql("SET spark.default.parallelism=20").collect()

    val contactPersonMerging = storage.readFromParquet[ContactPerson](config.matchingInputFile)

    val operatorIdAndRefs = storage
      .readFromParquet[Operator](config.operatorInputFile)
      .map(op ⇒ OHubIdAndRefId(op.ohubId.get, op.concatId)) // TODO resolve .get?

    val transformed = transform(spark, contactPersonMerging, operatorIdAndRefs)

    storage.writeToParquet(transformed, config.outputFile, partitionBy = Seq("countryCode"))
  }
}
