package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import com.unilever.ohub.spark.domain.entity.{ Activity, ContactPerson }
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

case class ActivityMergingConfig(
    contactPersonInputFile: String = "contact-person-input-file",
    activities: String = "activity-input-file",
    previousIntegrated: String = "previous-integrated-file",
    outputFile: String = "path-to-output-file"
) extends SparkJobConfig

object ActivityMerging extends SparkJob[ActivityMergingConfig] {

  def transform(
    spark: SparkSession,
    activities: Dataset[Activity],
    previousIntegrated: Dataset[Activity],
    contactPersons: Dataset[ContactPerson]
  ): Dataset[Activity] = {
    import spark.implicits._

    val allActivities = previousIntegrated
      .joinWith(activities, previousIntegrated("concatId") === activities("concatId"), JoinType.FullOuter)
      .map {
        case (integrated, activity) ⇒
          if (activity == null) {
            integrated
          } else if (integrated == null) {
            activity
          } else {
            activity.copy(ohubId = integrated.ohubId, ohubCreated = integrated.ohubCreated)
          }
      }

    allActivities
      .joinWith(contactPersons, $"contactPersonConcatId" === contactPersons("concatId"), "left")
      .map {
        case (order, cpn) ⇒
          if (cpn == null) order
          else order.copy(contactPersonOhubId = cpn.ohubId)
      }
  }

  override private[spark] def defaultConfig = ActivityMergingConfig()

  override private[spark] def configParser(): OptionParser[ActivityMergingConfig] =
    new scopt.OptionParser[ActivityMergingConfig]("Activity merging") {
      head("merges activities into an integrated activities output file.", "1.0")
      opt[String]("contactPersonInputFile") required () action { (x, c) ⇒
        c.copy(contactPersonInputFile = x)
      } text "contactPersonInputFile is a string property"
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
      s"Merging activities from [${config.activities}], [${config.contactPersonInputFile}] and [${config.previousIntegrated}] to [${config.outputFile}]"
    )

    val activities = storage.readFromParquet[Activity](config.activities)
    val previousIntegrated = storage.readFromParquet[Activity](config.previousIntegrated)
    val contactPersonRecords = storage.readFromParquet[ContactPerson](config.contactPersonInputFile)
    val transformed = transform(spark, activities, previousIntegrated, contactPersonRecords)

    storage.writeToParquet(transformed, config.outputFile)
  }
}
