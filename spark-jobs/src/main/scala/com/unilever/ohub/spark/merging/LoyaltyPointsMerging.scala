package com.unilever.ohub.spark.merging

import java.util.UUID

import com.unilever.ohub.spark.domain.entity.{ContactPerson, LoyaltyPoints, Operator}
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{SparkJob, SparkJobConfig}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.{Dataset, SQLImplicits, SparkSession}
import scopt.OptionParser
import org.apache.spark.sql.functions._

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

    val lp = previousIntegrated
      .joinWith(loyaltyPointsDS, previousIntegrated("concatId") === loyaltyPointsDS("concatId"), JoinType.FullOuter)
      // replace old
      .map {
        case (integrated, loyaltyPoints) =>
          if (loyaltyPoints == null) {
            integrated
          } else {
            val ohubId = if (integrated == null) Some(UUID.randomUUID().toString) else integrated.ohubId
            loyaltyPoints.copy(ohubId = ohubId)
          }
      }

    // update cpn ids
    val mergeLP = lp.joinWith(contactPersons, $"contactPersonConcatId" === contactPersons("concatId"), "left")
      .map {
        case (loyaltyPoints: LoyaltyPoints, cpn: ContactPerson) => loyaltyPoints.copy(contactPersonOhubId = cpn.ohubId)
        case (loyaltyPoints, _) => loyaltyPoints
      }
      // update opr ids
      .joinWith(operators, $"operatorConcatId" === operators("concatId"), "left")
      .map {
        case (loyaltyPoints: LoyaltyPoints, opr: Operator) => loyaltyPoints.copy(operatorOhubId = opr.ohubId)
        case (loyaltyPoints, _) => loyaltyPoints
      }.withColumn("isGoldenRecord", lit(false)).as[LoyaltyPoints]

    val goldenRecords = pickGoldenLP(mergeLP)(spark)

    mergeLP.join(goldenRecords, mergeLP("concatId") === goldenRecords("concatId"), JoinType.LeftAnti).as[LoyaltyPoints].unionByName(goldenRecords)
  }

  private def pickGoldenLP(loyaltyPoints: Dataset[LoyaltyPoints])(spark: SparkSession) = {
    import spark.implicits._
    val w = Window.partitionBy($"contactPersonOhubId").orderBy($"dateUpdated".desc_nulls_last, $"ohubUpdated".desc_nulls_last)
    loyaltyPoints.withColumn("rn", row_number.over(w))
      .filter($"rn" === 1)
      .drop($"rn").withColumn("isGoldenRecord", lit(true)).as[LoyaltyPoints]
  }

  override private[spark] def defaultConfig = LoyaltyPointsMergingConfig()

  override private[spark] def configParser(): OptionParser[LoyaltyPointsMergingConfig] =
    new scopt.OptionParser[LoyaltyPointsMergingConfig]("LoyaltyPoints merging") {
      head("merges loyaltyPoints into an integrated loyaltyPoints output file with referenced contactperson/operator.", "1.0")
      opt[String]("contactPersonIntegrated") required() action { (x, c) ⇒
        c.copy(contactPersonIntegrated = x)
      } text "contactPersonIntegrated is a string property"
      opt[String]("operatorIntegrated") required() action { (x, c) ⇒
        c.copy(operatorIntegrated = x)
      } text "operatorIntegrated is a string property"
      opt[String]("loyaltyPointsInputFile") required() action { (x, c) ⇒
        c.copy(loyaltyPoints = x)
      } text "loyaltyPointsInputFile is a string property"
      opt[String]("previousIntegrated") required() action { (x, c) ⇒
        c.copy(previousIntegrated = x)
      } text "previousIntegrated is a string property"
      opt[String]("outputFile") required() action { (x, c) ⇒
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
