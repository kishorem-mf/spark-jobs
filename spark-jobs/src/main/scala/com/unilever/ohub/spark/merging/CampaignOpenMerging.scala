package com.unilever.ohub.spark.merging

import java.util.UUID

import com.unilever.ohub.spark.domain.entity.{CampaignOpen, ContactPerson, Operator}
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{SparkJob, SparkJobConfig}
import org.apache.spark.sql.{Dataset, SparkSession}
import scopt.OptionParser

case class CampaignOpenMergingConfig(
    contactPersonGolden: String = "contact-person-integrated",
    operatorGolden: String = "operator-integrated",
    previousIntegrated: String = "previous-integrated-campaigns",
    campaignOpenInputFile: String = "campaign-open-input-file",
    outputFile: String = "path-to-output-file"
) extends SparkJobConfig

/**
 * Simple merging which only passes the latest version of a campaignOpen entity
 * (and copies the ohubId when it is previously integrated).
 */
object CampaignOpenMerging extends SparkJob[CampaignOpenMergingConfig] {

  def transform(
    spark: SparkSession,
    campaignOpens: Dataset[CampaignOpen],
    contactPersons: Dataset[ContactPerson],
    operators: Dataset[Operator],
    previousIntegrated: Dataset[CampaignOpen]
  ): Dataset[CampaignOpen] = {
    import spark.implicits._

    previousIntegrated
      .joinWith(campaignOpens, previousIntegrated("concatId") === campaignOpens("concatId"), JoinType.FullOuter)
      .map {
        case (integrated, campaignOpen) ⇒
          if (campaignOpen == null) {
            integrated
          } else {
            val ohubId = if (integrated == null) Some(UUID.randomUUID().toString) else integrated.ohubId
            campaignOpen.copy(ohubId = ohubId, isGoldenRecord = true)
          }
      }
      // update cpn ids
      .joinWith(contactPersons, $"contactPersonOhubId" === contactPersons("ohubId"), JoinType.Left)
      .map {
        case (open: CampaignOpen, cpn: ContactPerson) => open.copy(contactPersonConcatId = Some(cpn.concatId))
        case (open, _) => open
      }
      // update opr ids
      .joinWith(operators, $"operatorOhubId" === operators("ohubId"), JoinType.Left)
      .map {
        case (open: CampaignOpen, opr: Operator) => open.copy(operatorConcatId = Some(opr.concatId))
        case (open, _) => open
      }
  }

  override private[spark] def defaultConfig = CampaignOpenMergingConfig()

  override private[spark] def configParser(): OptionParser[CampaignOpenMergingConfig] =
    new scopt.OptionParser[CampaignOpenMergingConfig]("Order merging") {
      head("merges campaignsOpens into an integrated campaignOpens output file.", "1.0")
      opt[String]("contactPersonGolden") required () action { (x, c) ⇒
        c.copy(contactPersonGolden = x)
      } text "contactPersonGolden is a string property"
      opt[String]("operatorGolden") required () action { (x, c) ⇒
        c.copy(operatorGolden = x)
      } text "operatorGolden is a string property"
      opt[String]("campaignOpenInputFile") required () action { (x, c) ⇒
        c.copy(campaignOpenInputFile = x)
      } text "campaignOpenInputFile is a string property"
      opt[String]("previousIntegrated") optional () action { (x, c) ⇒
        c.copy(previousIntegrated = x)
      } text "previousIntegrated is a string property"
      opt[String]("outputFile") required () action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"

      version("1.0")
      help("help") text "help text"
    }

  override def run(spark: SparkSession, config: CampaignOpenMergingConfig, storage: Storage): Unit = {
    log.info(
      s"Merging campaignOpens from [${config.campaignOpenInputFile}] " +
        s"and [${config.previousIntegrated}] to [${config.outputFile}]"
    )

    val campaignOpenRecords = storage.readFromParquet[CampaignOpen](config.campaignOpenInputFile)
    val previousIntegrated = storage.readFromParquet[CampaignOpen](config.previousIntegrated)
    val contactPersons = storage.readFromParquet[ContactPerson](config.contactPersonGolden)
    val operators = storage.readFromParquet[Operator](config.operatorGolden)

    val transformed = transform(spark, campaignOpenRecords, contactPersons, operators, previousIntegrated)

    storage.writeToParquet(transformed, config.outputFile)
  }
}
