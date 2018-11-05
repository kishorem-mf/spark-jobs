package com.unilever.ohub.spark.merging

import java.sql.Timestamp

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity._
import org.apache.spark.sql.Dataset

class SubscriptionMergingSpec extends SparkJobSpec with TestSubscription with TestContactPersons {
  import spark.implicits._

  private val SUT = SubscriptionMerging

  describe("Subscription merging") {
    it("should take newest data if available while retaining ohubId") {
      val contactPersons = Seq(
        defaultContactPersonWithSourceEntityId("cpn1").copy(ohubId = Some("ohubCpn1")),
        defaultContactPersonWithSourceEntityId("cpn2").copy(ohubId = Some("ohubCpn2"))
      ).toDataset

      val updatedRecord = defaultSubscription.copy(
        isGoldenRecord = true,
        dateUpdated = Some(Timestamp.valueOf("2016-07-01 13:48:00.0")),
        ohubId = Some("oldId"),
        countryCode = "updated",
        concatId = s"updated~${defaultSubscription.sourceName}~${defaultSubscription.sourceEntityId}",
        contactPersonConcatId = "AU~WUFOO~cpn1"
      )

      val deletedRecord = defaultSubscription.copy(
        isGoldenRecord = true,
        dateUpdated = None,
        countryCode = "deleted",
        concatId = s"deleted~${defaultSubscription.sourceName}~${defaultSubscription.sourceEntityId}",
        contactPersonConcatId = "AU~WUFOO~cpn1",
        isActive = true)

      val newRecord = defaultSubscription.copy(
        isGoldenRecord = true,
        dateUpdated = Some(Timestamp.valueOf("2018-08-08 13:48:00.0")),
        countryCode = "new",
        concatId = s"new~${defaultSubscription.sourceName}~${defaultSubscription.sourceEntityId}",
        contactPersonConcatId = "AU~WUFOO~cpn1"
      )

      val unchangedRecord = defaultSubscription.copy(
        isGoldenRecord = true,
        countryCode = "unchanged",
        concatId = s"unchanged~${defaultSubscription.sourceName}~${defaultSubscription.sourceEntityId}",
        contactPersonConcatId = "AU~WUFOO~cpn2"
      )

      val notADeltaRecord = defaultSubscription.copy(
        isGoldenRecord = true,
        countryCode = "notADelta",
        concatId = s"notADelta~${defaultSubscription.sourceName}~${defaultSubscription.sourceEntityId}",
        contactPersonConcatId = "AU~WUFOO~cpn3"
      )

      val previous: Dataset[Subscription] = spark.createDataset(Seq(
        updatedRecord,
        deletedRecord,
        unchangedRecord,
        notADeltaRecord
      ))
      val input: Dataset[Subscription] = spark.createDataset(Seq(
        updatedRecord.copy(ohubId = Some("newId")),
        deletedRecord.copy(isActive = false),
        unchangedRecord,
        newRecord
      ))

      val result = SUT.transform(spark, input, previous, contactPersons)
        .collect()
        .sortBy(_.countryCode)

      result.length shouldBe 5
      assertSubscription(result(0), "deleted", isActive = false, isGoldenRecord = false, ohubId = "oldId", contactPersonOhubId = Some("ohubCpn1"))
      assertSubscription(result(1), "new", isActive = true, isGoldenRecord = true, ohubId = "oldId", contactPersonOhubId = Some("ohubCpn1"))
      assertSubscription(result(2), "notADelta", isActive = true, isGoldenRecord = true, ohubId = result(2).ohubId.get, contactPersonOhubId = None)
      assertSubscription(result(3), "unchanged", isActive = true, isGoldenRecord = true, ohubId = result(3).ohubId.get, contactPersonOhubId = Some("ohubCpn2"))
      assertSubscription(result(4), "updated", isActive = true, isGoldenRecord = false, ohubId = "oldId", contactPersonOhubId = Some("ohubCpn1"))
    }
  }

  private def assertSubscription(subscription: Subscription, countryCode: String, isActive: Boolean, isGoldenRecord: Boolean, ohubId: String, contactPersonOhubId: Option[String]): Unit = {
    subscription.countryCode shouldBe countryCode
    subscription.isActive shouldBe isActive
    subscription.ohubId shouldBe Some(ohubId)
    subscription.isGoldenRecord shouldBe isGoldenRecord
    subscription.contactPersonOhubId shouldBe contactPersonOhubId
  }
}
