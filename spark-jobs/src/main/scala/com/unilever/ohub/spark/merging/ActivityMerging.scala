package com.unilever.ohub.spark.merging

import java.util.UUID

import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import com.unilever.ohub.spark.domain.entity.{ Activity, ContactPerson, Operator }
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

case class ActivityMergingConfig(
    contactPersonIntegrated: String = "contact-person-integrated",
    operatorIntegrated: String = "operator-integrated",
    activities: String = "activity-input-file",
    previousIntegrated: String = "previous-integrated-file",
    outputFile: String = "path-to-output-file"
) extends SparkJobConfig

object ActivityMerging extends SparkJob[ActivityMergingConfig] {

  def transform(
    spark: SparkSession,
    activities: Dataset[Activity],
    previousIntegrated: Dataset[Activity],
    contactPersons: Dataset[ContactPerson],
    operators: Dataset[Operator]
  ): Dataset[Activity] = {
    import spark.implicits._

    previousIntegrated
      .joinWith(activities, previousIntegrated("concatId") === activities("concatId"), JoinType.FullOuter)
      // replace old
      .map {
        case (integrated, activity) ⇒
          if (activity == null) {
            integrated
          } else {
            val ohubId = if (integrated == null) Some(UUID.randomUUID().toString) else integrated.ohubId

            activity.copy(ohubId = ohubId, isGoldenRecord = true)
          }
      }
      // update cpn ids
      .joinWith(contactPersons, $"contactPersonConcatId" === contactPersons("concatId"), "left")
      .map {
        case (activity, cpn) ⇒
          if (cpn == null) activity
          else activity.copy(contactPersonOhubId = cpn.ohubId)
      }
      // update opr ids
      .joinWith(operators, $"operatorConcatId" === operators("concatId"), "left")
      .map {
        case (activity, opr) ⇒
          if (opr == null) activity
          else activity.copy(operatorOhubId = opr.ohubId)
      }
  }

  override private[spark] def defaultConfig = ActivityMergingConfig()

  override private[spark] def configParser(): OptionParser[ActivityMergingConfig] =
    new scopt.OptionParser[ActivityMergingConfig]("Activity merging") {
      head("merges activities into an integrated activities output file.", "1.0")
      opt[String]("contactPersonIntegrated") required () action { (x, c) ⇒
        c.copy(contactPersonIntegrated = x)
      } text "contactPersonIntegrated is a string property"
      opt[String]("operatorIntegrated") required () action { (x, c) ⇒
        c.copy(operatorIntegrated = x)
      } text "operatorIntegrated is a string property"
      opt[String]("activitiesInputFile") required () action { (x, c) ⇒
        c.copy(activities = x)
      } text "activitiesInputFile is a string property"
      opt[String]("previousIntegrated") required () action { (x, c) ⇒
        c.copy(previousIntegrated = x)
      } text "previousIntegrated is a string property"
      opt[String]("outputFile") required () action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"

      version("1.0")
      help("help") text "help text"
    }

  override def run(spark: SparkSession, config: ActivityMergingConfig, storage: Storage): Unit = {
    import spark.implicits._

    log.info(
      s"Merging activities from [${config.activities}], [${config.contactPersonIntegrated}], [${config.operatorIntegrated}] and [${config.previousIntegrated}] to [${config.outputFile}]"
    )

    val activities = storage.readFromParquet[Activity](config.activities)
    val previousIntegrated = storage.readFromParquet[Activity](config.previousIntegrated)
    val contactPersonRecords = storage.readFromParquet[ContactPerson](config.contactPersonIntegrated)
    val operatorRecords = storage.readFromParquet[Operator](config.operatorIntegrated)
    val transformed = transform(spark, activities, previousIntegrated, contactPersonRecords, operatorRecords)

    storage.writeToParquet(transformed, config.outputFile)
  }
}
