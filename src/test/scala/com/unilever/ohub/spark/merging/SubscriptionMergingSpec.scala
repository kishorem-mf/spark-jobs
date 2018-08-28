package com.unilever.ohub.spark.merging

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
        defaultContactPersonWithSourceEntityId("cpn2").copy(ohubId = Some("ohubCpn2")),
        defaultContactPersonWithSourceEntityId("cpn3").copy(ohubId = Some("ohubCpn3"))
      ).toDataset

      val updatedRecord = defaultSubscription.copy(
        isGoldenRecord = true,
        ohubId = Some("oldId"),
        countryCode = "updated",
        concatId = s"updated~${defaultSubscription.sourceName}~${defaultSubscription.sourceEntityId}",
        contactPersonConcatId = "AU~WUFOO~cpn1"
      )

      val deletedRecord = defaultSubscription.copy(
        isGoldenRecord = true,
        countryCode = "deleted",
        concatId = s"deleted~${defaultSubscription.sourceName}~${defaultSubscription.sourceEntityId}",
        contactPersonConcatId = "AU~WUFOO~cpn2",
        isActive = true)

      val newRecord = defaultSubscription.copy(
        isGoldenRecord = true,
        countryCode = "new",
        concatId = s"new~${defaultSubscription.sourceName}~${defaultSubscription.sourceEntityId}",
        contactPersonConcatId = "AU~WUFOO~cpn3"
      )

      val unchangedRecord = defaultSubscription.copy(
        isGoldenRecord = true,
        countryCode = "unchanged",
        concatId = s"unchanged~${defaultSubscription.sourceName}~${defaultSubscription.sourceEntityId}",
        contactPersonConcatId = "AU~WUFOO~cpn4"
      )

      val notADeltaRecord = defaultSubscription.copy(
        isGoldenRecord = true,
        countryCode = "notADelta",
        concatId = s"notADelta~${defaultSubscription.sourceName}~${defaultSubscription.sourceEntityId}",
        contactPersonConcatId = "AU~WUFOO~cpn5"
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
      result(0).isActive shouldBe false
      result(0).contactPersonOhubId shouldBe Some("ohubCpn2")

      result(1).countryCode shouldBe "new"
      result(1).contactPersonOhubId shouldBe Some("ohubCpn3")

      result(2).countryCode shouldBe "notADelta"
      result(2).contactPersonOhubId shouldBe None

      result(3).countryCode shouldBe "unchanged"
      result(3).contactPersonOhubId shouldBe None

      result(4).countryCode shouldBe "updated"
      result(4).contactPersonOhubId shouldBe Some("ohubCpn1")
      result(4).ohubId shouldBe Some("oldId")
    }
  }
}
