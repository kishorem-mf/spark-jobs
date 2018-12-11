package com.unilever.ohub.spark.merging

import java.util.UUID

import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import com.unilever.ohub.spark.domain.entity.{ ContactPerson, LoyaltyPoints, Operator }
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

case class LoyaltyPointsMergingConfig(
    contactPersonIntegrated: String = "contact-person-integrated",
    operatorIntegrated: String = "operator-integrated",
    loyaltyPoints: String = "loyalty-points-input-file",
    previousIntegrated: String = "previous-integrated-file",
    outputFile: String = "path-to-output-file"
) extends SparkJobConfig

object LoyaltyPointsMerging extends SparkJob[LoyaltyPointsMergingConfig] {

  def transform(
    spark: SparkSession,
    loyaltyPointsDS: Dataset[LoyaltyPoints],
    previousIntegrated: Dataset[LoyaltyPoints],
    contactPersons: Dataset[ContactPerson],
    operators: Dataset[Operator]
  ): Dataset[LoyaltyPoints] = {
    import spark.implicits._

    previousIntegrated
      .joinWith(loyaltyPointsDS, previousIntegrated("concatId") === loyaltyPointsDS("concatId"), JoinType.FullOuter)
      // replace old
      .map {
        case (integrated, loyaltyPoints) ⇒
          if (loyaltyPoints == null) integrated
          else {
            val ohubId = if (integrated == null) Some(UUID.randomUUID().toString) else integrated.ohubId

            loyaltyPoints.copy(ohubId = ohubId, isGoldenRecord = true)
          }
      }
      // update cpn ids
      .joinWith(contactPersons, $"contactPersonConcatId" === contactPersons("concatId"), "left")
      .map {
        case (loyaltyPoints, cpn) ⇒
          if (cpn == null) loyaltyPoints
          else loyaltyPoints.copy(contactPersonOhubId = cpn.ohubId)
      }
      // update opr ids
      .joinWith(operators, $"operatorConcatId" === operators("concatId"), "left")
      .map {
        case (loyaltyPoints, opr) ⇒
          if (opr == null) loyaltyPoints
          else loyaltyPoints.copy(operatorOhubId = opr.ohubId)
      }
  }

  override private[spark] def defaultConfig = LoyaltyPointsMergingConfig()

  override private[spark] def configParser(): OptionParser[LoyaltyPointsMergingConfig] =
    new scopt.OptionParser[LoyaltyPointsMergingConfig]("LoyaltyPoints merging") {
      head("merges loyaltyPoints into an integrated loyaltyPoints output file with referenced contactperson/operator.", "1.0")
      opt[String]("contactPersonIntegrated") required () action { (x, c) ⇒
        c.copy(contactPersonIntegrated = x)
      } text "contactPersonIntegrated is a string property"
      opt[String]("operatorIntegrated") required () action { (x, c) ⇒
        c.copy(operatorIntegrated = x)
      } text "operatorIntegrated is a string property"
      opt[String]("loyaltyPointsInputFile") required () action { (x, c) ⇒
        c.copy(loyaltyPoints = x)
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

  override def run(spark: SparkSession, config: LoyaltyPointsMergingConfig, storage: Storage): Unit = {
    log.info(
      s"Merging activities from [${config.loyaltyPoints}], [${config.contactPersonIntegrated}], [${config.operatorIntegrated}] and [${config.previousIntegrated}] to [${config.outputFile}]"
    )

    val loyaltyPoints = storage.readFromParquet[LoyaltyPoints](config.loyaltyPoints)
    val previousIntegrated = storage.readFromParquet[LoyaltyPoints](config.previousIntegrated)
    val contactPersonRecords = storage.readFromParquet[ContactPerson](config.contactPersonIntegrated)
    val operatorRecords = storage.readFromParquet[Operator](config.operatorIntegrated)
    val transformed = transform(spark, loyaltyPoints, previousIntegrated, contactPersonRecords, operatorRecords)

    storage.writeToParquet(transformed, config.outputFile)
  }
}
