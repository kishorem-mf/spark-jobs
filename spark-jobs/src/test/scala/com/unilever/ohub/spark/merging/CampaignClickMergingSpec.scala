package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity._
import org.apache.spark.sql.Dataset

class CampaignClickMergingSpec extends SparkJobSpec with TestCampaignClicks with TestContactPersons with TestOperators {

  import spark.implicits._

  private val SUT = CampaignClickMerging

  describe("CampaignClick merging") {

    it("should give a new campaignClick an ohubId and be marked golden record") {
      val input = Seq(
        defaultCampaignClick
      ).toDataset

      val contactPersons = Seq[ContactPerson]().toDataset
      val operators = Seq[Operator]().toDataset
      val previous = Seq[CampaignClick]().toDataset

      val result = SUT.transform(spark, input, contactPersons, operators, previous)
        .collect()

      result.head.ohubId shouldBe defined
      result.head.isGoldenRecord shouldBe true
    }

    it("should take newest data if available while retaining ohubId") {

      val contactPersons = Seq[ContactPerson]().toDataset
      val operators = Seq[Operator]().toDataset

      val updatedRecord = defaultCampaignClick.copy(
        isGoldenRecord = true,
        ohubId = Some("oldId"),
        countryCode = "updated",
        concatId = s"updated~${defaultCampaignClick.sourceName}~${defaultCampaignClick.sourceEntityId}")

      val newRecord = defaultCampaignClick.copy(
        isGoldenRecord = true,
        countryCode = "new",
        concatId = s"new~${defaultCampaignClick.sourceName}~${defaultCampaignClick.sourceEntityId}"
      )

      val unchangedRecord = defaultCampaignClick.copy(
        isGoldenRecord = true,
        countryCode = "unchanged",
        concatId = s"unchanged~${defaultCampaignClick.sourceName}~${defaultCampaignClick.sourceEntityId}"
      )

      val notADeltaRecord = defaultCampaignClick.copy(
        isGoldenRecord = true,
        countryCode = "notADelta",
        concatId = s"notADelta~${defaultCampaignClick.sourceName}~${defaultCampaignClick.sourceEntityId}"
      )

      val previous: Dataset[CampaignClick] = spark.createDataset(Seq(
        updatedRecord,
        unchangedRecord,
        notADeltaRecord
      ))

      val input: Dataset[CampaignClick] = spark.createDataset(Seq(
        updatedRecord.copy(ohubId = Some("newId")),
        unchangedRecord,
        newRecord
      ))

      val result = SUT.transform(spark, input, contactPersons, operators, previous)
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
      val previous = Seq[CampaignClick]().toDataset

      val contactPerson = defaultContactPerson.copy(
        concatId = "123",
        ohubId = Some("456")
      )

      val operator = defaultOperator.copy(
        concatId = "789",
        ohubId = Some("101112")
      )

      val updatedRecord = defaultCampaignClick.copy(
        contactPersonConcatId = "123",
        operatorConcatId = Some("789")
      )

      val contactPersons = spark.createDataset(Seq(contactPerson))

      val operators = spark.createDataset(Seq(operator))

      val input = spark.createDataset(Seq(updatedRecord))

      val result = SUT.transform(spark, input, contactPersons, operators, previous).collect()

      result.length shouldBe 1
      result(0).contactPersonOhubId shouldBe Some("456")
      result(0).operatorOhubId shouldBe Some("101112")
    }
  }
}
