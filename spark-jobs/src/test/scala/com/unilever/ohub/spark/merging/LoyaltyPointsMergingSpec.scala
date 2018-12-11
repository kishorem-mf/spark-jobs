package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity._
import org.apache.spark.sql.Dataset

class LoyaltyPointsMergingSpec extends SparkJobSpec with TestLoyaltyPoints with TestContactPersons with TestOperators {

  import spark.implicits._

  private val SUT = LoyaltyPointsMerging;

  describe("LoyaltyPoints merging") {

    it("should give a new loyaltyPoints an ohubId and be marked golden record") {
      val input = Seq(
        defaultLoyaltyPoints
      ).toDataset

      val previous = Seq[LoyaltyPoints]().toDataset
      val contactPersons = Seq[ContactPerson]().toDataset
      val operators = Seq[Operator]().toDataset

      val result = SUT.transform(spark, input, previous, contactPersons, operators)
        .collect()

      result.head.ohubId shouldBe defined
      result.head.isGoldenRecord shouldBe true
    }

    it("should set the ohubId references to a contact person and operator") {
      val input = Seq(
        defaultLoyaltyPoints.copy(
          contactPersonConcatId = Some("DE~FILE~CP1"),
          contactPersonOhubId = None,
          operatorConcatId = Some("DE~FILE~OP1"),
          operatorOhubId = None
        )
      ).toDataset

      val previous = Seq[LoyaltyPoints]().toDataset
      val contactPersons = Seq[ContactPerson](
        defaultContactPerson.copy(
          concatId = "DE~FILE~CP1",
          ohubId = Some("OHUB_ID_CP_1")
        )
      ).toDataset
      val operators = Seq[Operator](
        defaultOperator.copy(
          concatId = "DE~FILE~OP1",
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

      val updatedRecord = defaultLoyaltyPoints.copy(
        isGoldenRecord = true,
        ohubId = Some("oldId"),
        countryCode = "updated",
        contactPersonConcatId = Some("AU~WUFOO~cpn1"),
        concatId = s"updated~${defaultLoyaltyPoints.sourceName}~${defaultLoyaltyPoints.sourceEntityId}")

      val deletedRecord = defaultLoyaltyPoints.copy(
        isGoldenRecord = true,
        countryCode = "deleted",
        contactPersonConcatId = Some("AU~WUFOO~cpn2"),
        concatId = s"deleted~${defaultLoyaltyPoints.sourceName}~${defaultLoyaltyPoints.sourceEntityId}",
        isActive = true)

      val newRecord = defaultLoyaltyPoints.copy(
        isGoldenRecord = true,
        countryCode = "new",
        contactPersonConcatId = Some("AU~WUFOO~cpn3"),
        concatId = s"new~${defaultLoyaltyPoints.sourceName}~${defaultLoyaltyPoints.sourceEntityId}"
      )

      val unchangedRecord = defaultLoyaltyPoints.copy(
        isGoldenRecord = true,
        countryCode = "unchanged",
        contactPersonConcatId = Some("AU~WUFOO~cpn4"),
        concatId = s"unchanged~${defaultLoyaltyPoints.sourceName}~${defaultLoyaltyPoints.sourceEntityId}"
      )

      val notADeltaRecord = defaultLoyaltyPoints.copy(
        isGoldenRecord = true,
        countryCode = "notADelta",
        contactPersonConcatId = Some("AU~WUFOO~cpn5"),
        concatId = s"notADelta~${defaultLoyaltyPoints.sourceName}~${defaultLoyaltyPoints.sourceEntityId}"
      )

      val previous: Dataset[LoyaltyPoints] = spark.createDataset(Seq(
        updatedRecord,
        deletedRecord,
        unchangedRecord,
        notADeltaRecord
      ))

      val input: Dataset[LoyaltyPoints] = spark.createDataset(Seq(
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
