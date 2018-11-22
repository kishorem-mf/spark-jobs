package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity._
import org.apache.spark.sql.Dataset
import com.unilever.ohub.spark.SharedSparkSession.spark

class ActivityMergingSpec extends SparkJobSpec with TestActivities with TestContactPersons with TestOperators {

  import spark.implicits._

  private val SUT = ActivityMerging

  describe("Activity merging") {

    it("should give a new activity an ohubId and be marked golden record") {
      val input = Seq(
        defaultActivity
      ).toDataset

      val previous = Seq[Activity]().toDataset
      val contactPersons = Seq[ContactPerson]().toDataset
      val operators = Seq[Operator]().toDataset

      val result = SUT.transform(spark, input, previous, contactPersons, operators)
        .collect()

      result.head.ohubId shouldBe defined
      result.head.isGoldenRecord shouldBe true
    }

    it("should set the ohubId references to a contact person and operator") {
      val input = Seq(
        defaultActivity.copy(
          contactPersonConcatId = Some("DE~SUBSCRIPTION~CP1"),
          contactPersonOhubId = None,
          operatorConcatId = Some("DE~SUBSCRIPTION~OP1"),
          operatorOhubId = None
        )
      ).toDataset

      val previous = Seq[Activity]().toDataset
      val contactPersons = Seq[ContactPerson](
        defaultContactPerson.copy(
          concatId = "DE~SUBSCRIPTION~CP1",
          ohubId = Some("OHUB_ID_CP_1")
        )
      ).toDataset
      val operators = Seq[Operator](
        defaultOperator.copy(
          concatId = "DE~SUBSCRIPTION~OP1",
          ohubId = Some("OHUB_ID_OP_1")
        )
      ).toDataset

      val result = SUT.transform(spark, input, previous, contactPersons, operators)
        .collect()
      result.head.contactPersonOhubId shouldBe Some("OHUB_ID_CP_1")
      result.head.operatorOhubId shouldBe Some("OHUB_ID_OP_1")
    }

    it("should take newest data if available while retaining ohubId") {

      val contactPersons = Seq(
        defaultContactPersonWithSourceEntityId("cpn1").copy(ohubId = Some("ohubCpn1")),
        defaultContactPersonWithSourceEntityId("cpn2").copy(ohubId = Some("ohubCpn2")),
        defaultContactPersonWithSourceEntityId("cpn3").copy(ohubId = Some("ohubCpn3"))
      ).toDataset

      val operators = Seq(
        defaultOperator
      ).toDataset

      val updatedRecord = defaultActivity.copy(
        isGoldenRecord = true,
        ohubId = Some("oldId"),
        countryCode = "updated",
        contactPersonConcatId = Some("AU~WUFOO~cpn1"),
        concatId = s"updated~${defaultActivity.sourceName}~${defaultActivity.sourceEntityId}")

      val deletedRecord = defaultActivity.copy(
        isGoldenRecord = true,
        countryCode = "deleted",
        contactPersonConcatId = Some("AU~WUFOO~cpn2"),
        concatId = s"deleted~${defaultActivity.sourceName}~${defaultActivity.sourceEntityId}",
        isActive = true)

      val newRecord = defaultActivity.copy(
        isGoldenRecord = true,
        countryCode = "new",
        contactPersonConcatId = Some("AU~WUFOO~cpn3"),
        concatId = s"new~${defaultActivity.sourceName}~${defaultActivity.sourceEntityId}"
      )

      val unchangedRecord = defaultActivity.copy(
        isGoldenRecord = true,
        countryCode = "unchanged",
        contactPersonConcatId = Some("AU~WUFOO~cpn4"),
        concatId = s"unchanged~${defaultActivity.sourceName}~${defaultActivity.sourceEntityId}"
      )

      val notADeltaRecord = defaultActivity.copy(
        isGoldenRecord = true,
        countryCode = "notADelta",
        contactPersonConcatId = Some("AU~WUFOO~cpn5"),
        concatId = s"notADelta~${defaultActivity.sourceName}~${defaultActivity.sourceEntityId}"
      )

      val previous: Dataset[Activity] = spark.createDataset(Seq(
        updatedRecord,
        deletedRecord,
        unchangedRecord,
        notADeltaRecord
      ))

      val input: Dataset[Activity] = spark.createDataset(Seq(
        updatedRecord.copy(ohubId = Some("newId")),
        deletedRecord.copy(isActive = false),
        unchangedRecord,
        newRecord
      ))

      val result = SUT.transform(spark, input, previous, contactPersons, operators)
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
