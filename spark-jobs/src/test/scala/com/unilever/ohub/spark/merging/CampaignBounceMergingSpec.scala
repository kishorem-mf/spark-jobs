package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity._
import org.apache.spark.sql.Dataset

class CampaignBounceMergingSpec extends SparkJobSpec with TestCampaignBounces with TestContactPersons with TestOperators {

  import spark.implicits._

  private val SUT = CampaignBounceMerging

  describe("CampaignBounce merging") {

    it("should give a new campaignBounce an ohubId and be marked golden record") {
      val input = Seq(
        defaultCampaignBounce
      ).toDataset

      val contactPersons = Seq[ContactPerson]().toDataset
      val operators = Seq[Operator]().toDataset
      val previous = Seq[CampaignBounce]().toDataset

      val result = SUT.transform(spark, input, contactPersons, operators, previous)
        .collect()

      result.head.ohubId shouldBe defined
      result.head.isGoldenRecord shouldBe true
    }

    it("should take newest data if available while retaining ohubId") {

      val contactPersons = Seq[ContactPerson]().toDataset
      val operators = Seq[Operator]().toDataset

      val updatedRecord = defaultCampaignBounce.copy(
        isGoldenRecord = true,
        ohubId = Some("oldId"),
        countryCode = "updated",
        concatId = s"updated~${defaultCampaignBounce.sourceName}~${defaultCampaignBounce.sourceEntityId}")

      val newRecord = defaultCampaignBounce.copy(
        isGoldenRecord = true,
        countryCode = "new",
        concatId = s"new~${defaultCampaignBounce.sourceName}~${defaultCampaignBounce.sourceEntityId}"
      )

      val unchangedRecord = defaultCampaignBounce.copy(
        isGoldenRecord = true,
        countryCode = "unchanged",
        concatId = s"unchanged~${defaultCampaignBounce.sourceName}~${defaultCampaignBounce.sourceEntityId}"
      )

      val notADeltaRecord = defaultCampaignBounce.copy(
        isGoldenRecord = true,
        countryCode = "notADelta",
        concatId = s"notADelta~${defaultCampaignBounce.sourceName}~${defaultCampaignBounce.sourceEntityId}"
      )

      val previous: Dataset[CampaignBounce] = spark.createDataset(Seq(
        updatedRecord,
        unchangedRecord,
        notADeltaRecord
      ))

      val input: Dataset[CampaignBounce] = spark.createDataset(Seq(
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

    it("should copy concatId from contactperson and operator") {
      val previous = Seq[CampaignBounce]().toDataset

      val contactPerson = defaultContactPerson.copy(
        concatId = "123",
        ohubId = Some("456")
      )

      val operator = defaultOperator.copy(
        concatId = "789",
        ohubId = Some("101112")
      )

      val updatedRecord = defaultCampaignBounce.copy(
        contactPersonOhubId = "456",
        operatorOhubId = Some("101112")
      )

      val contactPersons = spark.createDataset(Seq(contactPerson))

      val operators = spark.createDataset(Seq(operator))

      val input = spark.createDataset(Seq(updatedRecord))

      val result = SUT.transform(spark, input, contactPersons, operators, previous).collect()

      result.length shouldBe 1
      result(0).contactPersonConcatId shouldBe Some("123")
      result(0).operatorConcatId shouldBe Some("789")
    }
  }
}
