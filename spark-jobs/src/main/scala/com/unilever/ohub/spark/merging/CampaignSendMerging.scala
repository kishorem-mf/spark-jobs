package com.unilever.ohub.spark.merging

import java.util.UUID

import com.unilever.ohub.spark.domain.entity.Campaign
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{SparkJob, SparkJobConfig}
import org.apache.spark.sql.{Dataset, SparkSession}
import scopt.OptionParser

case class CampaignSendMergingConfig(
   previousIntegrated: String = "previous-integrated-campaigns",
   campaignSendInputFile: String = "campaign-send-input-file",
   outputFile: String = "path-to-output-file"
) extends SparkJobConfig

/**
  * Simple merging which only passes the latest version of a campaignSend entity
  * (and copies the ohubId when it is previously integrated).
  */
object CampaignSendMerging extends SparkJob[CampaignSendMergingConfig] {

  def transform(
     spark: SparkSession,
     campaigns: Dataset[Campaign],
     previousIntegrated: Dataset[Campaign]
  ): Dataset[Campaign] = {
    import spark.implicits._

    previousIntegrated
      .joinWith(campaigns, previousIntegrated("concatId") === campaigns("concatId"), JoinType.FullOuter)
      .map {
        case (integrated, campaignSend) ⇒
          if (campaignSend == null) {
            integrated
          } else {
            val ohubId = if (integrated == null) Some(UUID.randomUUID().toString) else integrated.ohubId
            campaignSend.copy(ohubId = ohubId, isGoldenRecord = true)
          }
      }
  }

  override private[spark] def defaultConfig = CampaignSendMergingConfig()

  override private[spark] def configParser(): OptionParser[CampaignSendMergingConfig] =
    new scopt.OptionParser[CampaignSendMergingConfig]("Order merging") {
      head("merges campaignsSends into an integrated campaignSends output file.", "1.0")
      opt[String]("campaignInputFile") required () action { (x, c) ⇒
        c.copy(campaignSendInputFile = x)
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

  override def run(spark: SparkSession, config: CampaignSendMergingConfig, storage: Storage): Unit = {
    log.info(
      s"Merging campaigns from [${config.campaignSendInputFile}] " +
        s"and [${config.previousIntegrated}] to [${config.outputFile}]"
    )

    val campaignRecords = storage.readFromParquet[Campaign](config.campaignSendInputFile)
    val previousIntegrated = storage.readFromParquet[Campaign](config.previousIntegrated)

    val transformed = transform(spark, campaignRecords, previousIntegrated)

    storage.writeToParquet(transformed, config.outputFile)
  }
}
