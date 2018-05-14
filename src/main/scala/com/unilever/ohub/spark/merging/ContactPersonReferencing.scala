package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import com.unilever.ohub.spark.domain.entity.{ ContactPerson, Operator }
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

case class ContactPersonRefsConfig(
    combinedInputFile: String = "combined-input-file",
    operatorInputFile: String = "operator-input-file",
    outputFile: String = "path-to-output-file"
) extends SparkJobConfig

// The step that fixes the foreign key links between contact persons and operators
object ContactPersonReferencing extends SparkJob[ContactPersonRefsConfig] {
  private case class OHubIdAndRefId(ohubId: Option[String], refId: String)

  def transform(
    spark: SparkSession,
    contactPersons: Dataset[ContactPerson],
    operatorIdAndRefs: Dataset[OHubIdAndRefId]
  ): Dataset[ContactPerson] = {
    import spark.implicits._

    contactPersons
      .joinWith(
        operatorIdAndRefs,
        operatorIdAndRefs("refId") === contactPersons("contactPerson.operatorConcatId"),
        JoinType.LeftOuter
      )
      .map {
        case (contactPerson, maybeOperator) ⇒
          contactPerson.copy(operatorOhubId = maybeOperator.ohubId)
      }
  }

  override private[spark] def defaultConfig = ContactPersonRefsConfig()

  override private[spark] def configParser(): OptionParser[ContactPersonRefsConfig] =
    new scopt.OptionParser[ContactPersonRefsConfig]("Contact person merging 2") {
      head("resolves refs from contact persons to operators.", "1.0")
      opt[String]("combinedInputFile") required () action { (x, c) ⇒
        c.copy(combinedInputFile = x)
      } text "combinedInputFile is a string property"
      opt[String]("operatorInputFile") required () action { (x, c) ⇒
        c.copy(operatorInputFile = x)
      } text "operatorInputFile is a string property"
      opt[String]("outputFile") required () action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"

      version("1.0")
      help("help") text "help text"
    }

  override def run(spark: SparkSession, config: ContactPersonRefsConfig, storage: Storage): Unit = {
    import spark.implicits._

    log.info(
      s"Set contact person references for [${config.combinedInputFile}] " +
        s"to [${config.operatorInputFile}] " +
        s"and write result to [${config.outputFile}]"
    )

    // TODO run some performance tests on how this runs on a cluster VS forced repartition
    spark.sql("SET spark.sql.shuffle.partitions=20").collect()
    spark.sql("SET spark.default.parallelism=20").collect()

    val contactPersons = storage.readFromParquet[ContactPerson](config.combinedInputFile)

    val operatorIdAndRefs = storage
      .readFromParquet[Operator](config.operatorInputFile)
      .map(op ⇒ OHubIdAndRefId(op.ohubId, op.concatId))

    val transformed = transform(spark, contactPersons, operatorIdAndRefs)

    storage.writeToParquet(transformed, config.outputFile, partitionBy = Seq("countryCode"))
  }
}
