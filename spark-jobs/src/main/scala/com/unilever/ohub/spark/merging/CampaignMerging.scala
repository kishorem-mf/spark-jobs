package com.unilever.ohub.spark.merging

import java.util.UUID

import com.unilever.ohub.spark.domain.entity.{Campaign, ContactPerson, Operator}
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{SparkJob, SparkJobConfig}
import org.apache.spark.sql.{Dataset, SparkSession}
import scopt.OptionParser

case class CampaignMergingConfig(
   contactPersonIntegrated: String = "contact-person-integrated",
   operatorIntegrated: String = "operator-integrated",
   previousIntegrated: String = "previous-integrated-campaigns",
   campaignInputFile: String = "campaign-input-file",
   outputFile: String = "path-to-output-file"
) extends SparkJobConfig

/**
  * Simple merging which only passes the latest version of a campaign entity
  * (and copies the ohubId when it is previously integrated).
  */
object CampaignMerging extends SparkJob[CampaignMergingConfig] {

  def transform(
     spark: SparkSession,
     campaigns: Dataset[Campaign],
     contactPersons: Dataset[ContactPerson],
     operators: Dataset[Operator],
     previousIntegrated: Dataset[Campaign]
  ): Dataset[Campaign] = {
    import spark.implicits._

    previousIntegrated
      .joinWith(campaigns, previousIntegrated("concatId") === campaigns("concatId"), JoinType.FullOuter)
      .map {
        case (integrated, campaign) ⇒
          if (campaign == null) {
            integrated
          } else {
            val ohubId = if (integrated == null) Some(UUID.randomUUID().toString) else integrated.ohubId
            campaign.copy(ohubId = ohubId, isGoldenRecord = true)
          }
      }
      // update cpn ids
      .joinWith(contactPersons, $"contactPersonConcatId" === contactPersons("concatId"), JoinType.Left)
      .map {
        case (campaign, cpn) ⇒
          if (cpn == null) campaign
          else campaign.copy(contactPersonOhubId = cpn.ohubId)
      }
      // update opr ids
      .joinWith(operators, $"operatorConcatId" === operators("concatId"), JoinType.Left)
      .map {
        case (campaign, opr) ⇒
          if (opr == null) campaign
          else campaign.copy(operatorOhubId = opr.ohubId)
      }
  }

  override private[spark] def defaultConfig = CampaignMergingConfig()

  override private[spark] def configParser(): OptionParser[CampaignMergingConfig] =
    new scopt.OptionParser[CampaignMergingConfig]("Order merging") {
      head("merges campaigns into an integrated campaigns output file.", "1.0")
      opt[String]("contactPersonIntegrated") required () action { (x, c) ⇒
        c.copy(contactPersonIntegrated = x)
      } text "contactPersonIntegrated is a string property"
      opt[String]("operatorIntegrated") required () action { (x, c) ⇒
        c.copy(operatorIntegrated = x)
      } text "operatorIntegrated is a string property"
      opt[String]("campaignInputFile") required () action { (x, c) ⇒
        c.copy(campaignInputFile = x)
      } text "campaignInputFile is a string property"
      opt[String]("previousIntegrated") optional () action { (x, c) ⇒
        c.copy(previousIntegrated = x)
      } text "previousIntegrated is a string property"
      opt[String]("outputFile") required () action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"

      version("1.0")
      help("help") text "help text"
    }

  override def run(spark: SparkSession, config: CampaignMergingConfig, storage: Storage): Unit = {
    log.info(
      s"Merging campaigns from [${config.campaignInputFile}] " +
        s"and [${config.previousIntegrated}] to [${config.outputFile}]"
    )

    val campaignRecords = storage.readFromParquet[Campaign](config.campaignInputFile)
    val previousIntegrated = storage.readFromParquet[Campaign](config.previousIntegrated)
    val contactPersons = storage.readFromParquet[ContactPerson](config.contactPersonIntegrated)
    val operators = storage.readFromParquet[Operator](config.operatorIntegrated)

    val transformed = transform(spark, campaignRecords, contactPersons, operators, previousIntegrated)

    storage.writeToParquet(transformed, config.outputFile)
  }
}
