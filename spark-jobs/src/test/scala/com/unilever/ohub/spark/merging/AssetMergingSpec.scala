package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity._
import org.apache.spark.sql.Dataset
import com.unilever.ohub.spark.SharedSparkSession.spark

class AssetMergingSpec extends SparkJobSpec with TestAssets {

  import spark.implicits._

  private val SUT = AssetMerging

  describe("Asset merging") {

    it("should give a new asset an ohubId and be marked golden record") {
      val input = Seq(
        defaultAsset
      ).toDataset

      val previous = Seq[Asset]().toDataset

      val result = SUT.transform(spark, input, previous)
        .collect()

      result.head.ohubId shouldBe defined
      result.head.isGoldenRecord shouldBe true
    }

    it("should take newest data if available while retaining ohubId") {

      val updatedRecord = defaultAsset.copy(
        isGoldenRecord = true,
        ohubId = Some("oldId"),
        countryCode = "updated",
        concatId = s"updated~${defaultAsset.sourceName}~${defaultAsset.sourceEntityId}")

      val deletedRecord = defaultAsset.copy(
        isGoldenRecord = true,
        countryCode = "deleted",
        concatId = s"deleted~${defaultAsset.sourceName}~${defaultAsset.sourceEntityId}",
        isActive = true)

      val newRecord = defaultAsset.copy(
        isGoldenRecord = true,
        countryCode = "new",
        concatId = s"new~${defaultAsset.sourceName}~${defaultAsset.sourceEntityId}"
      )

      val unchangedRecord = defaultAsset.copy(
        isGoldenRecord = true,
        countryCode = "unchanged",
        concatId = s"unchanged~${defaultAsset.sourceName}~${defaultAsset.sourceEntityId}"
      )

      val notADeltaRecord = defaultAsset.copy(
        isGoldenRecord = true,
        countryCode = "notADelta",
        concatId = s"notADelta~${defaultAsset.sourceName}~${defaultAsset.sourceEntityId}"
      )

      val previous: Dataset[Asset] = spark.createDataset(Seq(
        updatedRecord,
        deletedRecord,
        unchangedRecord,
        notADeltaRecord
      ))

      val input: Dataset[Asset] = spark.createDataset(Seq(
        updatedRecord.copy(ohubId = Some("newId")),
        deletedRecord.copy(isActive = false),
        unchangedRecord,
        newRecord
      ))

      val result = SUT.transform(spark, input, previous)
        .collect()
        .sortBy(_.countryCode)

      result.length shouldBe 5
      result(0).isActive shouldBe false

      result(1).countryCode shouldBe "new"

      result(2).countryCode shouldBe "notADelta"

      result(3).countryCode shouldBe "unchanged"

      result(4).countryCode shouldBe "updated"
      result(4).ohubId shouldBe Some("oldId")
    }
  }
}
