package com.unilever.ohub.spark.merging

import java.util.UUID

import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import com.unilever.ohub.spark.domain.entity.{ ContactPerson, Subscription }
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.sql.JoinType
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{ collect_list, row_number }
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

case class SubscriptionMergingConfig(
    contactPersonInputFile: String = "contact-person-input-file",
    previousIntegrated: String = "previous-integrated-subscriptions",
    subscriptionInputFile: String = "subscription-input-file",
    outputFile: String = "path-to-output-file"
) extends SparkJobConfig

case class ContactPersonConcatIdAndRecord(contactPersonConcatId: String, subscription: Subscription)

// Technically not really subscription MERGING, but we need to update foreign key IDs in the other records
object SubscriptionMerging extends SparkJob[SubscriptionMergingConfig] {

  def groupWithOhubId: Seq[Subscription] ⇒ Seq[Subscription] = subscriptions ⇒ {
    val ohubId: String = subscriptions
      .find(_.ohubId.isDefined)
      .flatMap(_.ohubId)
      .getOrElse(UUID.randomUUID().toString)

    subscriptions.map(_.copy(ohubId = Some(ohubId)))
  }

  def transform(
    spark: SparkSession,
    subscriptions: Dataset[Subscription],
    previousIntegrated: Dataset[Subscription],
    contactPersons: Dataset[ContactPerson]
  ): Dataset[Subscription] = {
    import spark.implicits._

    val allSubscriptions =
      previousIntegrated
        .joinWith(subscriptions, previousIntegrated("concatId") === subscriptions("concatId"), JoinType.FullOuter)
        .map {
          case (integrated, delta) ⇒
            if (delta == null) {
              integrated
            } else if (integrated == null) {
              delta
            } else {
              delta.copy(ohubId = integrated.ohubId) // preserve ohubId's
            }
        }

    val w = Window.partitionBy($"contactPersonConcatId").orderBy($"dateUpdated".desc_nulls_last)

    val allSubscriptionsGrouped =
      allSubscriptions
        .withColumn("rn", row_number.over(w))
        .withColumn("isGoldenRecord", $"rn" === 1)
        .drop($"rn")
        .as[Subscription]
        .map(s ⇒ ContactPersonConcatIdAndRecord(s.contactPersonConcatId, s))
        .groupBy("contactPersonConcatId")
        .agg(collect_list("subscription").as("subscriptions"))
        .as[(String, Seq[Subscription])]
        .map(_._2)
        .flatMap(groupWithOhubId)

    allSubscriptionsGrouped
      .joinWith(contactPersons, $"contactPersonConcatId" === contactPersons("concatId"), JoinType.Left)
      .map {
        case (subscription, cpn) ⇒
          if (cpn == null) subscription
          else subscription.copy(contactPersonOhubId = cpn.ohubId)
      }
  }

  override private[spark] def defaultConfig = SubscriptionMergingConfig()

  override private[spark] def configParser(): OptionParser[SubscriptionMergingConfig] =
    new scopt.OptionParser[SubscriptionMergingConfig]("Subscriptions merging") {
      head("merges susbscriptions into an integrated susbscriptions output file.", "1.0")
      opt[String]("contactPersonInputFile") required () action { (x, c) ⇒
        c.copy(contactPersonInputFile = x)
      } text "contactPersonInputFile is a string property"
      opt[String]("subscriptionInputFile") required () action { (x, c) ⇒
        c.copy(subscriptionInputFile = x)
      } text "subscriptionInputFile is a string property"
      opt[String]("previousIntegrated") optional () action { (x, c) ⇒
        c.copy(previousIntegrated = x)
      } text "previousIntegrated is a string property"
      opt[String]("outputFile") required () action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"

      version("1.0")
      help("help") text "help text"
    }

  override def run(spark: SparkSession, config: SubscriptionMergingConfig, storage: Storage): Unit = {
    import spark.implicits._

    log.info(
      s"Merging subscriptions from [${config.contactPersonInputFile}] " +
        s"and [${config.subscriptionInputFile}] to [${config.outputFile}]"
    )

    val subscriptionRecords = storage.readFromParquet[Subscription](config.subscriptionInputFile)
    val contactPersonRecords = storage.readFromParquet[ContactPerson](config.contactPersonInputFile)
    val previousIntegrated = storage.readFromParquet[Subscription](config.previousIntegrated)

    val transformed = transform(spark, subscriptionRecords, previousIntegrated, contactPersonRecords)

    storage.writeToParquet(transformed, config.outputFile)
  }
}
