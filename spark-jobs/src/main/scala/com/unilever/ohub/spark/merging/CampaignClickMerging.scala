package com.unilever.ohub.spark.merging

import java.util.UUID

import com.unilever.ohub.spark.domain.entity.{CampaignClick, ContactPerson, Operator}
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{SparkJob, SparkJobConfig}
import org.apache.spark.sql.{Dataset, SparkSession}
import scopt.OptionParser

case class CampaignClickMergingConfig(
                                       contactPersonIntegrated: String = "contact-person-integrated",
                                       operatorIntegrated: String = "operator-integrated",
                                       previousIntegrated: String = "previous-integrated-campaigns",
                                       campaignClickInputFile: String = "campaign-click-input-file",
                                       outputFile: String = "path-to-output-file"
                                     ) extends SparkJobConfig

/**
 * Simple merging which only passes the latest version of a campaignClick entity
 * (and copies the ohubId when it is previously integrated).
 */
object CampaignClickMerging extends SparkJob[CampaignClickMergingConfig] {

  def transform(
                 spark: SparkSession,
                 campaignClicks: Dataset[CampaignClick],
                 contactPersons: Dataset[ContactPerson],
                 operators: Dataset[Operator],
                 previousIntegrated: Dataset[CampaignClick]
               ): Dataset[CampaignClick] = {
    import spark.implicits._

    previousIntegrated
      .joinWith(campaignClicks, previousIntegrated("concatId") === campaignClicks("concatId"), JoinType.FullOuter)
      .map {
        case (integrated, campaignClick) ⇒
          if (campaignClick == null) {
            integrated
          } else {
            val ohubId = if (integrated == null) Some(UUID.randomUUID().toString) else integrated.ohubId
            campaignClick.copy(ohubId = ohubId, isGoldenRecord = true)
          }
      }
      // update cpn ids
      .joinWith(contactPersons, $"contactPersonConcatId" === contactPersons("concatId"), JoinType.Left)
      .map {
        case (click: CampaignClick, cpn: ContactPerson) ⇒ click.copy(contactPersonOhubId = cpn.ohubId)
        case (click, _) => click
      }
      // update opr ids
      .joinWith(operators, $"operatorConcatId" === operators("concatId"), JoinType.Left)
      .map {
        case (click: CampaignClick, opr: Operator) ⇒ click.copy(operatorOhubId = opr.ohubId)
        case (click, _) => click
      }
  }

  override private[spark] def defaultConfig = CampaignClickMergingConfig()

  override private[spark] def configParser(): OptionParser[CampaignClickMergingConfig] =
    new scopt.OptionParser[CampaignClickMergingConfig]("Order merging") {
      head("merges campaignsClicks into an integrated campaignClicks output file.", "1.0")
      opt[String]("contactPersonIntegrated") required() action { (x, c) ⇒
        c.copy(contactPersonIntegrated = x)
      } text "contactPersonIntegrated is a string property"
      opt[String]("operatorIntegrated") required() action { (x, c) ⇒
        c.copy(operatorIntegrated = x)
      } text "operatorIntegrated is a string property"
      opt[String]("campaignClickInputFile") required() action { (x, c) ⇒
        c.copy(campaignClickInputFile = x)
      } text "campaignClickInputFile is a string property"
      opt[String]("previousIntegrated") optional() action { (x, c) ⇒
        c.copy(previousIntegrated = x)
      } text "previousIntegrated is a string property"
      opt[String]("outputFile") required() action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"

      version("1.0")
      help("help") text "help text"
    }

  override def run(spark: SparkSession, config: CampaignClickMergingConfig, storage: Storage): Unit = {
    log.info(
      s"Merging campaignClicks from [${config.campaignClickInputFile}] " +
        s"and [${config.previousIntegrated}] to [${config.outputFile}]"
    )

    val campaignClickRecords = storage.readFromParquet[CampaignClick](config.campaignClickInputFile)
    val previousIntegrated = storage.readFromParquet[CampaignClick](config.previousIntegrated)
    val contactPersons = storage.readFromParquet[ContactPerson](config.contactPersonIntegrated)
    val operators = storage.readFromParquet[Operator](config.operatorIntegrated)

    val transformed = transform(spark, campaignClickRecords, contactPersons, operators, previousIntegrated)

    storage.writeToParquet(transformed, config.outputFile)
  }
}
