package com.unilever.ohub.spark.merging

import java.sql.Timestamp

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity._
import org.apache.spark.sql.Dataset

class SubscriptionMergingSpec extends SparkJobSpec with TestSubscription with TestContactPersons {
  import spark.implicits._

  private val SUT = SubscriptionMerging

  val contactPersons = Seq(
    defaultContactPerson.copy(concatId = "cpn1", ohubId = Some("ohubCpn1")),
    defaultContactPerson.copy(concatId = "cpn2", ohubId = Some("ohubCpn1"))
  ).toDataset

  describe("Subscription merging") {
    it("should mark the most recent based on subscriptionDate golden record") {
      val previous: Dataset[Subscription] = spark.createDataset(Seq())
      val input: Dataset[Subscription] = spark.createDataset(Seq(
        defaultSubscription.copy(
          concatId = "s1",
          isGoldenRecord = false,
          ohubId = None,
          contactPersonConcatId = "cpn1",
          contactPersonOhubId = None,
          subscriptionType = "default_newsletter_opt_in",
          subscriptionDate = Timestamp.valueOf("2018-11-27 11:45:00.0"),
          confirmedSubscriptionDate = None
        ),
        defaultSubscription.copy(
          concatId = "s2",
          isGoldenRecord = false,
          ohubId = None,
          contactPersonConcatId = "cpn2",
          contactPersonOhubId = None,
          subscriptionType = "default_newsletter_opt_in",
          subscriptionDate = Timestamp.valueOf("2018-11-28 11:45:00.0"),
          confirmedSubscriptionDate = None
        )
      ))

      val result = SUT.transform(spark, input, previous, contactPersons).collect()

      result.map(_.ohubId).distinct.size shouldBe 1
      result.map(s ⇒ s.concatId -> s.isGoldenRecord).toSet shouldBe Set(
        ("s1", false),
        ("s2", true)
      )
    }

    it("should use confirmedSubscriptionDate for ordering subscriptions when available") {
      val previous: Dataset[Subscription] = spark.createDataset(Seq())
      val input: Dataset[Subscription] = spark.createDataset(Seq(
        defaultSubscription.copy(
          concatId = "s1",
          isGoldenRecord = false,
          ohubId = None,
          contactPersonConcatId = "cpn1",
          contactPersonOhubId = None,
          subscriptionType = "default_newsletter_opt_in",
          subscriptionDate = Timestamp.valueOf("2018-11-26 11:45:00.0"),
          confirmedSubscriptionDate = Some(Timestamp.valueOf("2018-11-28 11:45:00.0"))
        ),
        defaultSubscription.copy(
          concatId = "s2",
          isGoldenRecord = false,
          ohubId = None,
          contactPersonConcatId = "cpn2",
          contactPersonOhubId = None,
          subscriptionType = "default_newsletter_opt_in",
          subscriptionDate = Timestamp.valueOf("2018-11-27 11:45:00.0"),
          confirmedSubscriptionDate = Some(Timestamp.valueOf("2018-11-25 11:45:00.0"))
        )
      ))

      val result = SUT.transform(spark, input, previous, contactPersons).collect()

      result.map(_.ohubId).distinct.size shouldBe 1
      result.map(s ⇒ s.concatId -> s.isGoldenRecord).toSet shouldBe Set(
        ("s1", true),
        ("s2", false)
      )
    }

    it("should mark subscriptions that differ in subscriptionType golden record") {
      val previous: Dataset[Subscription] = spark.createDataset(Seq())
      val input: Dataset[Subscription] = spark.createDataset(Seq(
        defaultSubscription.copy(
          concatId = "s1",
          isGoldenRecord = false,
          ohubId = None,
          contactPersonConcatId = "cpn1",
          contactPersonOhubId = None,
          subscriptionType = "type_1",
          subscriptionDate = Timestamp.valueOf("2018-11-27 11:45:00.0"),
          confirmedSubscriptionDate = None
        ),
        defaultSubscription.copy(
          concatId = "s2",
          isGoldenRecord = false,
          ohubId = None,
          contactPersonConcatId = "cpn2",
          contactPersonOhubId = None,
          subscriptionType = "type_2",
          subscriptionDate = Timestamp.valueOf("2018-11-28 11:45:00.0"),
          confirmedSubscriptionDate = None
        )
      ))

      val result = SUT.transform(spark, input, previous, contactPersons).collect()

      result.map(_.ohubId).distinct.size shouldBe 2
      result.map(s ⇒ s.concatId -> s.isGoldenRecord).toSet shouldBe Set(
        ("s1", true),
        ("s2", true)
      )
    }

    it("should preserve ohubIds") {
      val previous: Dataset[Subscription] = spark.createDataset(Seq(
        defaultSubscription.copy(
          concatId = "s1",
          isGoldenRecord = true,
          ohubId = Some("ohub-id-1"),
          contactPersonConcatId = "cpn1",
          contactPersonOhubId = Some("ohubCpn1"),
          subscriptionType = "default_newsletter_opt_in",
          subscriptionDate = Timestamp.valueOf("2018-11-27 11:45:00.0"),
          confirmedSubscriptionDate = None
        )
      ))
      val input: Dataset[Subscription] = spark.createDataset(Seq(
        defaultSubscription.copy(
          concatId = "s1",
          isGoldenRecord = false,
          ohubId = None,
          contactPersonConcatId = "cpn1",
          contactPersonOhubId = None,
          subscriptionType = "default_newsletter_opt_in",
          subscriptionDate = Timestamp.valueOf("2018-11-27 11:45:00.0"),
          confirmedSubscriptionDate = None
        ),
        defaultSubscription.copy(
          concatId = "s2",
          isGoldenRecord = false,
          ohubId = None,
          contactPersonConcatId = "cpn2",
          contactPersonOhubId = None,
          subscriptionType = "default_newsletter_opt_in",
          subscriptionDate = Timestamp.valueOf("2018-11-28 11:45:00.0"),
          confirmedSubscriptionDate = None
        )
      ))

      val result = SUT.transform(spark, input, previous, contactPersons).collect()

      result.map(_.ohubId).distinct.toSet shouldBe Set(Some("ohub-id-1"))
      result.map(s ⇒ s.concatId -> s.isGoldenRecord).toSet shouldBe Set(
        ("s1", false),
        ("s2", true)
      )
    }

    it("should handle deletes correctly") { // delete is a special case of updates
      val previous: Dataset[Subscription] = spark.createDataset(Seq(
        defaultSubscription.copy(
          concatId = "s1",
          isGoldenRecord = false,
          ohubId = Some("ohub-id-1"),
          contactPersonConcatId = "cpn1",
          contactPersonOhubId = Some("ohubCpn1"),
          subscriptionType = "default_newsletter_opt_in",
          subscriptionDate = Timestamp.valueOf("2018-11-27 11:45:00.0"),
          confirmedSubscriptionDate = None
        )
      ))
      val input: Dataset[Subscription] = spark.createDataset(Seq(
        defaultSubscription.copy(
          concatId = "s1",
          isGoldenRecord = false,
          isActive = false,
          ohubId = None,
          contactPersonConcatId = "cpn1",
          contactPersonOhubId = None,
          subscriptionType = "default_newsletter_opt_in",
          subscriptionDate = Timestamp.valueOf("2018-11-27 11:45:00.0"),
          confirmedSubscriptionDate = None
        )
      ))

      val result = SUT.transform(spark, input, previous, contactPersons).collect()

      result.map(_.ohubId).distinct.toSet shouldBe Set(Some("ohub-id-1"))
      result.map(s ⇒ (s.concatId, s.isGoldenRecord, s.isActive)).toSet shouldBe Set(
        ("s1", true, false)
      )
    }

    it("should not loose records") {
      val previous: Dataset[Subscription] = spark.createDataset(Seq(
        defaultSubscription.copy(
          concatId = "s1",
          isGoldenRecord = true,
          ohubId = Some("ohub-id-1"),
          contactPersonConcatId = "cpn1",
          contactPersonOhubId = Some("ohubCpn1"),
          subscriptionType = "type_1",
          subscriptionDate = Timestamp.valueOf("2018-11-27 11:45:00.0"),
          confirmedSubscriptionDate = None
        ),
        defaultSubscription.copy(
          concatId = "s2",
          isGoldenRecord = true,
          ohubId = Some("ohub-id-2"),
          contactPersonConcatId = "cpn1",
          contactPersonOhubId = Some("ohubCpn1"),
          subscriptionType = "type_2",
          subscriptionDate = Timestamp.valueOf("2018-11-27 11:45:00.0"),
          confirmedSubscriptionDate = None
        )
      ))
      val input: Dataset[Subscription] = spark.createDataset(Seq(
        defaultSubscription.copy(
          concatId = "s1",
          isGoldenRecord = false,
          ohubId = None,
          contactPersonConcatId = "cpn1",
          contactPersonOhubId = None,
          subscriptionType = "type_1",
          subscriptionDate = Timestamp.valueOf("2018-11-27 11:45:00.0"),
          confirmedSubscriptionDate = None
        ),
        defaultSubscription.copy(
          concatId = "s3",
          isGoldenRecord = false,
          ohubId = None,
          contactPersonConcatId = "cpn1",
          contactPersonOhubId = None,
          subscriptionType = "type_2",
          subscriptionDate = Timestamp.valueOf("2018-11-28 11:45:00.0"),
          confirmedSubscriptionDate = None
        ),
        defaultSubscription.copy(
          concatId = "s4",
          isGoldenRecord = false,
          ohubId = None,
          contactPersonConcatId = "cpn3",
          contactPersonOhubId = None,
          subscriptionType = "type_2",
          subscriptionDate = Timestamp.valueOf("2018-11-28 11:45:00.0"),
          confirmedSubscriptionDate = None
        )
      ))

      val result = SUT.transform(spark, input, previous, contactPersons).collect()

      result.map(s ⇒ (s.concatId, s.isGoldenRecord, s.ohubId, s.contactPersonOhubId)).toSet shouldBe Set(
        ("s1", true, Some("ohub-id-1"), Some("ohubCpn1")),
        ("s2", false, Some("ohub-id-2"), Some("ohubCpn1")),
        ("s3", true, Some("ohub-id-2"), Some("ohubCpn1")),
        ("s4", true, result.filter(_.concatId == "s4").head.ohubId, None)
      )
    }
  }
}
