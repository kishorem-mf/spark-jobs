package com.unilever.ohub.spark.merging

import java.util.UUID

import com.unilever.ohub.spark.domain.entity.{CampaignBounce, ContactPerson, Operator}
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{SparkJob, SparkJobConfig}
import org.apache.spark.sql.{Dataset, SparkSession}
import scopt.OptionParser

case class CampaignBounceMergingConfig(
                                        contactPersonGolden: String = "contact-person-integrated",
                                        operatorGolden: String = "operator-integrated",
                                        previousIntegrated: String = "previous-integrated-campaigns",
                                        campaignBounceInputFile: String = "campaign-bounce-input-file",
                                        outputFile: String = "path-to-output-file"
                                      ) extends SparkJobConfig

/**
 * Simple merging which only passes the latest version of a campaignBounce entity
 * (and copies the ohubId when it is previously integrated).
 */
object CampaignBounceMerging extends SparkJob[CampaignBounceMergingConfig] {

  def transform(
                 spark: SparkSession,
                 campaignBounces: Dataset[CampaignBounce],
                 contactPersons: Dataset[ContactPerson],
                 operators: Dataset[Operator],
                 previousIntegrated: Dataset[CampaignBounce]
               ): Dataset[CampaignBounce] = {
    import spark.implicits._

    previousIntegrated
      .joinWith(campaignBounces, previousIntegrated("concatId") === campaignBounces("concatId"), JoinType.FullOuter)
      .map {
        case (integrated, campaignBounce) ⇒
          if (campaignBounce == null) {
            integrated
          } else {
            val ohubId = if (integrated == null) Some(UUID.randomUUID().toString) else integrated.ohubId
            campaignBounce.copy(ohubId = ohubId, isGoldenRecord = true)
          }
      }
      // update cpn ids
      .joinWith(contactPersons, $"contactPersonOhubId" === contactPersons("ohubId"), JoinType.Left)
      .map {
        case (bounce: CampaignBounce, cpn: ContactPerson) => bounce.copy(contactPersonConcatId = Some(cpn.concatId))
        case (bounce, _) ⇒ bounce
      }
      // update opr ids
      .joinWith(operators, $"operatorOhubId" === operators("ohubId"), JoinType.Left)
      .map {
        case (bounce: CampaignBounce, opr: Operator) ⇒ bounce.copy(operatorConcatId = Some(opr.concatId))
        case (bounce, _) => bounce
      }
  }

  override private[spark] def defaultConfig = CampaignBounceMergingConfig()

  override private[spark] def configParser(): OptionParser[CampaignBounceMergingConfig] =
    new scopt.OptionParser[CampaignBounceMergingConfig]("Order merging") {
      head("merges campaignsBounces into an integrated campaignBounces output file.", "1.0")
      opt[String]("contactPersonGolden") required() action { (x, c) ⇒
        c.copy(contactPersonGolden = x)
      } text "contactPersonGolden is a string property"
      opt[String]("operatorGolden") required() action { (x, c) ⇒
        c.copy(operatorGolden = x)
      } text "operatorGolden is a string property"
      opt[String]("campaignBounceInputFile") required() action { (x, c) ⇒
        c.copy(campaignBounceInputFile = x)
      } text "campaignBounceInputFile is a string property"
      opt[String]("previousIntegrated") optional() action { (x, c) ⇒
        c.copy(previousIntegrated = x)
      } text "previousIntegrated is a string property"
      opt[String]("outputFile") required() action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"

      version("1.0")
      help("help") text "help text"
    }

  override def run(spark: SparkSession, config: CampaignBounceMergingConfig, storage: Storage): Unit = {
    log.info(
      s"Merging campaignBounces from [${config.campaignBounceInputFile}] " +
        s"and [${config.previousIntegrated}] to [${config.outputFile}]"
    )

    val campaignBounceRecords = storage.readFromParquet[CampaignBounce](config.campaignBounceInputFile)
    val previousIntegrated = storage.readFromParquet[CampaignBounce](config.previousIntegrated)
    val contactPersons = storage.readFromParquet[ContactPerson](config.contactPersonGolden)
    val operators = storage.readFromParquet[Operator](config.operatorGolden)

    val transformed = transform(spark, campaignBounceRecords, contactPersons, operators, previousIntegrated)

    storage.writeToParquet(transformed, config.outputFile)
  }
}
