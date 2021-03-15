package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity._
import org.apache.spark.sql.Dataset

class WholesalerAssignmentMergingSpec extends SparkJobSpec with TestWholesalerAssignments with TestOperators {

  import spark.implicits._

  private val SUT = WholesalerAssignmentMerging

  describe("WholesalerAssignment merging") {

    it("should give a new wholesalerassignment an ohubId and be marked golden record") {
      val input = Seq(
        defaultWholesalerAssignment
      ).toDataset

      val previous = Seq[WholesalerAssignment]().toDataset
      val operators = Seq[Operator]().toDataset

      val result = SUT.transform(spark, input, previous, operators)
        .collect()

      result.head.ohubId shouldBe defined
      result.head.isGoldenRecord shouldBe true
    }

    it("should set the ohubId references to an operator") {
      val input = Seq(
        defaultWholesalerAssignment.copy(
          operatorConcatId = Some("DE~EMAKINA~789"),
          operatorOhubId = None
        )
      ).toDataset

      val previous = Seq[WholesalerAssignment]().toDataset
      val operators = Seq[Operator](
        defaultOperator.copy(
          concatId = "DE~EMAKINA~789",
          ohubId = Some("OHUB_ID_OP_1")
        )
      ).toDataset

      val result = SUT.transform(spark, input, previous, operators)
        .collect()
      result.head.operatorOhubId shouldBe Some("OHUB_ID_OP_1")
    }

    it("should take newest data if available while retaining ohubId") {

      val operators = Seq(
        defaultOperator
      ).toDataset

      val updatedRecord = defaultWholesalerAssignment.copy(
        isGoldenRecord = true,
        ohubId = Some("oldId"),
        countryCode = "updated",
        concatId = s"updated~${defaultWholesalerAssignment.sourceName}~${defaultWholesalerAssignment.sourceEntityId}")

      val deletedRecord = defaultWholesalerAssignment.copy(
        isGoldenRecord = true,
        countryCode = "deleted",
        concatId = s"deleted~${defaultWholesalerAssignment.sourceName}~${defaultWholesalerAssignment.sourceEntityId}",
        isActive = true)

      val newRecord = defaultWholesalerAssignment.copy(
        isGoldenRecord = true,
        countryCode = "new",
        concatId = s"new~${defaultWholesalerAssignment.sourceName}~${defaultWholesalerAssignment.sourceEntityId}"
      )

      val unchangedRecord = defaultWholesalerAssignment.copy(
        isGoldenRecord = true,
        countryCode = "unchanged",
        concatId = s"unchanged~${defaultWholesalerAssignment.sourceName}~${defaultWholesalerAssignment.sourceEntityId}"
      )

      val notADeltaRecord = defaultWholesalerAssignment.copy(
        isGoldenRecord = true,
        countryCode = "notADelta",
        concatId = s"notADelta~${defaultWholesalerAssignment.sourceName}~${defaultWholesalerAssignment.sourceEntityId}"
      )

      val previous: Dataset[WholesalerAssignment] = spark.createDataset(Seq(
        updatedRecord,
        deletedRecord,
        unchangedRecord,
        notADeltaRecord
      ))

      val input: Dataset[WholesalerAssignment] = spark.createDataset(Seq(
        updatedRecord.copy(ohubId = Some("newId")),
        deletedRecord.copy(isActive = false),
        unchangedRecord,
        newRecord
      ))

      val result = SUT.transform(spark, input, previous, operators)
        .collect()
        .sortBy(_.countryCode)

      result.length shouldBe 5
      result(0).isActive shouldBe false
      result(1).countryCode shouldBe "new"
      result(2).countryCode shouldBe "notADelta"
      result(3).countryCode shouldBe "unchanged"
      result(4).countryCode shouldBe "updated"
    }
  }
}
