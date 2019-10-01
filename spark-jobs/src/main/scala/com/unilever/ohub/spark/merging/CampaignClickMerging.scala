package com.unilever.ohub.spark.merging

import java.util.UUID

import com.unilever.ohub.spark.domain.entity.{CampaignClick, ContactPerson, Operator}
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{SparkJob, SparkJobConfig}
import org.apache.spark.sql.{Dataset, SparkSession}
import scopt.OptionParser

case class CampaignClickMergingConfig(
                                       contactPersonGolden: String = "contact-person-integrated",
                                       operatorGolden: String = "operator-integrated",
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
      .joinWith(contactPersons, $"contactPersonOhubId" === contactPersons("ohubId"), JoinType.Left)
      .map {
        case (click: CampaignClick, cpn: ContactPerson) ⇒ click.copy(contactPersonConcatId = Some(cpn.concatId))
        case (click, _) => click
      }
      // update opr ids
      .joinWith(operators, $"operatorOhubId" === operators("ohubId"), JoinType.Left)
      .map {
        case (click: CampaignClick, opr: Operator) ⇒ click.copy(operatorConcatId = Some(opr.concatId))
        case (click, _) => click
      }
  }

  override private[spark] def defaultConfig = CampaignClickMergingConfig()

  override private[spark] def configParser(): OptionParser[CampaignClickMergingConfig] =
    new scopt.OptionParser[CampaignClickMergingConfig]("Order merging") {
      head("merges campaignsClicks into an integrated campaignClicks output file.", "1.0")
      opt[String]("contactPersonGolden") required() action { (x, c) ⇒
        c.copy(contactPersonGolden = x)
      } text "contactPersonGolden is a string property"
      opt[String]("operatorGolden") required() action { (x, c) ⇒
        c.copy(operatorGolden = x)
      } text "operatorGolden is a string property"
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
    val contactPersons = storage.readFromParquet[ContactPerson](config.contactPersonGolden)
    val operators = storage.readFromParquet[Operator](config.operatorGolden)

    val transformed = transform(spark, campaignClickRecords, contactPersons, operators, previousIntegrated)

    storage.writeToParquet(transformed, config.outputFile)
  }
}
