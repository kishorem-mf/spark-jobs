package com.unilever.ohub.spark.merging

import java.util.UUID

import com.unilever.ohub.spark.domain.entity.CampaignSend
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
     campaignSends: Dataset[CampaignSend],
     previousIntegrated: Dataset[CampaignSend]
  ): Dataset[CampaignSend] = {
    import spark.implicits._

    previousIntegrated
      .joinWith(campaignSends, previousIntegrated("concatId") === campaignSends("concatId"), JoinType.FullOuter)
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
      s"Merging campaignSends from [${config.campaignSendInputFile}] " +
        s"and [${config.previousIntegrated}] to [${config.outputFile}]"
    )

    val campaignSendRecords = storage.readFromParquet[CampaignSend](config.campaignSendInputFile)
    val previousIntegrated = storage.readFromParquet[CampaignSend](config.previousIntegrated)

    val transformed = transform(spark, campaignSendRecords, previousIntegrated)

    storage.writeToParquet(transformed, config.outputFile)
  }
}
