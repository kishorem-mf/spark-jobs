package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity._
import org.apache.spark.sql.Dataset

class CampaignMergingSpec extends SparkJobSpec with TestCampaigns with TestContactPersons with TestOperators {

  import spark.implicits._

  private val SUT = CampaignMerging

  describe("Campaign merging") {

    it("should give a new campaign an ohubId and be marked golden record") {
      val input = Seq(
        defaultCampaign
      ).toDataset

      val contactPersons = Seq[ContactPerson]().toDataset
      val previous = Seq[Campaign]().toDataset

      val result = SUT.transform(spark, input, contactPersons, previous)
        .collect()

      result.head.ohubId shouldBe defined
      result.head.isGoldenRecord shouldBe true
    }

    it("should take newest data if available while retaining ohubId") {
      val contactPersons = Seq[ContactPerson]().toDataset

      val updatedRecord = defaultCampaign.copy(
        isGoldenRecord = true,
        ohubId = Some("oldId"),
        countryCode = "updated",
        concatId = s"updated~${defaultCampaign.sourceName}~${defaultCampaign.sourceEntityId}")

      val newRecord = defaultCampaign.copy(
        isGoldenRecord = true,
        countryCode = "new",
        concatId = s"new~${defaultCampaign.sourceName}~${defaultCampaign.sourceEntityId}"
      )

      val unchangedRecord = defaultCampaign.copy(
        isGoldenRecord = true,
        countryCode = "unchanged",
        concatId = s"unchanged~${defaultCampaign.sourceName}~${defaultCampaign.sourceEntityId}"
      )

      val notADeltaRecord = defaultCampaign.copy(
        isGoldenRecord = true,
        countryCode = "notADelta",
        concatId = s"notADelta~${defaultCampaign.sourceName}~${defaultCampaign.sourceEntityId}"
      )

      val previous: Dataset[Campaign] = spark.createDataset(Seq(
        updatedRecord,
        unchangedRecord,
        notADeltaRecord
      ))

      val input: Dataset[Campaign] = spark.createDataset(Seq(
        updatedRecord.copy(ohubId = Some("newId")),
        unchangedRecord,
        newRecord
      ))

      val result = SUT.transform(spark, input, contactPersons, previous)
        .collect()
        .sortBy(_.countryCode)

      result.length shouldBe 4
      result(0).countryCode shouldBe "new"

      result(1).countryCode shouldBe "notADelta"

      result(2).countryCode shouldBe "unchanged"

      result(3).countryCode shouldBe "updated"
      result(3).ohubId shouldBe Some("oldId")
    }

    it("should copy ohubId from contactperson and operator") {
      val previous = Seq[Campaign]().toDataset

      val contactPerson = defaultContactPerson.copy(
        concatId = "123",
        ohubId = Some("456")
      )

      val updatedRecord = defaultCampaign.copy(
        contactPersonConcatId = "123"
      )

      val contactPersons = spark.createDataset(Seq(contactPerson))

      val input = spark.createDataset(Seq(updatedRecord))

      val result = SUT.transform(spark, input, contactPersons, previous).collect()

      result.length shouldBe 1
      result(0).contactPersonOhubId shouldBe Some("456")
    }
  }
}
