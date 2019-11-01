package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity._
import org.apache.spark.sql.Dataset

class CampaignSendMergingSpec extends SparkJobSpec with TestCampaignSends with TestContactPersons with TestOperators {

  import spark.implicits._

  private val SUT = CampaignSendMerging

  describe("CampaignSend merging") {

    it("should give a new campaignSend an ohubId and be marked golden record") {
      val input = Seq(
        defaultCampaignSend
      ).toDataset

      val contactPersons = Seq[ContactPerson]().toDataset
      val operators = Seq[Operator]().toDataset
      val previous = Seq[CampaignSend]().toDataset

      val result = SUT.transform(spark, input, contactPersons, operators, previous)
        .collect()

      result.head.ohubId shouldBe defined
      result.head.isGoldenRecord shouldBe true
    }

    it("should take newest data if available while retaining ohubId") {
      val contactPersons = Seq[ContactPerson]().toDataset
      val operators = Seq[Operator]().toDataset

      val updatedRecord = defaultCampaignSend.copy(
        isGoldenRecord = true,
        ohubId = Some("oldId"),
        countryCode = "updated",
        concatId = s"updated~${defaultCampaignSend.sourceName}~${defaultCampaignSend.sourceEntityId}")

      val newRecord = defaultCampaignSend.copy(
        isGoldenRecord = true,
        countryCode = "new",
        concatId = s"new~${defaultCampaignSend.sourceName}~${defaultCampaignSend.sourceEntityId}"
      )

      val unchangedRecord = defaultCampaignSend.copy(
        isGoldenRecord = true,
        countryCode = "unchanged",
        concatId = s"unchanged~${defaultCampaignSend.sourceName}~${defaultCampaignSend.sourceEntityId}"
      )

      val notADeltaRecord = defaultCampaignSend.copy(
        isGoldenRecord = true,
        countryCode = "notADelta",
        concatId = s"notADelta~${defaultCampaignSend.sourceName}~${defaultCampaignSend.sourceEntityId}"
      )

      val previous: Dataset[CampaignSend] = spark.createDataset(Seq(
        updatedRecord,
        unchangedRecord,
        notADeltaRecord
      ))

      val input: Dataset[CampaignSend] = spark.createDataset(Seq(
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
      val previous = Seq[CampaignSend]().toDataset

      val contactPerson = defaultContactPerson.copy(
        concatId = "123",
        ohubId = Some("456")
      )

      val operator = defaultOperator.copy(
        concatId = "789",
        ohubId = Some("101112")
      )

      val updatedRecord = defaultCampaignSend.copy(
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
