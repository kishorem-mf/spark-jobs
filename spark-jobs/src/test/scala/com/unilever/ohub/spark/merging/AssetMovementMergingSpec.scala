package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity._
import org.apache.spark.sql.Dataset

class AssetMovementMergingSpec extends SparkJobSpec with TestAssetMovements with TestOperators {

  import spark.implicits._

  private val SUT = AssetMovementMerging

  describe("AssetMovement merging") {

    it("should give a new AssetMovement an ohubId and be marked golden record") {
      val input = Seq(
        defaultAssetMovement
      ).toDataset

      val previous = Seq[AssetMovement]().toDataset
      val operators = Seq[Operator]().toDataset

      val result = SUT.transform(spark, input, previous, operators)
        .collect()

      result.head.ohubId shouldBe defined
      result.head.isGoldenRecord shouldBe true
    }

    it("should set the ohubId references to an operator") {
      val input = Seq(
        defaultAssetMovement.copy(
          operatorConcatId = Some("DE~EMAKINA~123"),
          operatorOhubId = None
        )
      ).toDataset

      val previous = Seq[AssetMovement]().toDataset
      val operators = Seq[Operator](
        defaultOperator.copy(
          concatId = "DE~EMAKINA~123",
          ohubId = Some("OHUBID_1")
        )
      ).toDataset

      val result = SUT.transform(spark, input, previous, operators)
        .collect()
      result.head.operatorOhubId shouldBe Some("OHUBID_1")
    }

    it("should take newest data if available while retaining ohubId") {

      val operators = Seq(
        defaultOperator
      ).toDataset

      val updatedRecord = defaultAssetMovement.copy(
        isGoldenRecord = true,
        ohubId = Some("oldId"),
        countryCode = "updated",
        concatId = s"updated~${defaultAssetMovement.sourceName}~${defaultAssetMovement.sourceEntityId}")

      val deletedRecord = defaultAssetMovement.copy(
        isGoldenRecord = true,
        countryCode = "deleted",
        concatId = s"deleted~${defaultAssetMovement.sourceName}~${defaultAssetMovement.sourceEntityId}",
        isActive = true)

      val newRecord = defaultAssetMovement.copy(
        isGoldenRecord = true,
        countryCode = "new",
        concatId = s"new~${defaultAssetMovement.sourceName}~${defaultAssetMovement.sourceEntityId}"
      )

      val unchangedRecord = defaultAssetMovement.copy(
        isGoldenRecord = true,
        countryCode = "unchanged",
        concatId = s"unchanged~${defaultAssetMovement.sourceName}~${defaultAssetMovement.sourceEntityId}"
      )

      val notADeltaRecord = defaultAssetMovement.copy(
        isGoldenRecord = true,
        countryCode = "notADelta",
        concatId = s"notADelta~${defaultAssetMovement.sourceName}~${defaultAssetMovement.sourceEntityId}"
      )

      val previous: Dataset[AssetMovement] = spark.createDataset(Seq(
        updatedRecord,
        deletedRecord,
        unchangedRecord,
        notADeltaRecord
      ))

      val input: Dataset[AssetMovement] = spark.createDataset(Seq(
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
