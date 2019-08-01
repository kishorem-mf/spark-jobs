package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.{Chain, TestChains}
import org.apache.spark.sql.Dataset

class ChainMergingTest extends SparkJobSpec with TestChains {

  import spark.implicits._

  private val SUT = ChainMerging

  describe("chain merging") {
    it("a new chain should get an ohubId and be marked golden record") {
      val newRecord = defaultChain.copy(
        concatId = "new",
        isGoldenRecord = false,
        ohubId = None
      )

      val delta = Seq(
        newRecord
      ).toDataset

      val integrated = Seq[Chain]().toDataset

      val result = SUT.transform(spark, delta, integrated).collect()

      result.size shouldBe 1
      result.head.ohubId shouldBe defined
      result.head.isGoldenRecord shouldBe true
    }

    it("should take newest data if available while retaining ohubId") {
      val updatedRecord = defaultChain.copy(
        isGoldenRecord = true,
        ohubId = Some("oldId"),
        countryCode = "updated",
        concatId = s"updated~${defaultChain.sourceName}~${defaultChain.sourceEntityId}")

      val deletedRecord = defaultChain.copy(
        isGoldenRecord = true,
        countryCode = "deleted",
        concatId = s"deleted~${defaultChain.sourceName}~${defaultChain.sourceEntityId}",
        isActive = true)

      val newRecord = defaultChain.copy(
        isGoldenRecord = true,
        countryCode = "new",
        concatId = s"new~${defaultChain.sourceName}~${defaultChain.sourceEntityId}"
      )

      val unchangedRecord = defaultChain.copy(
        isGoldenRecord = true,
        countryCode = "unchanged",
        concatId = s"unchanged~${defaultChain.sourceName}~${defaultChain.sourceEntityId}"
      )

      val notADeltaRecord = defaultChain.copy(
        isGoldenRecord = true,
        countryCode = "notADelta",
        concatId = s"notADelta~${defaultChain.sourceName}~${defaultChain.sourceEntityId}"
      )

      val previous: Dataset[Chain] = spark.createDataset(Seq(
        updatedRecord,
        deletedRecord,
        unchangedRecord,
        notADeltaRecord
      ))
      val input: Dataset[Chain] = spark.createDataset(Seq(
        updatedRecord.copy(conceptName = Some("Test"), ohubId = Some("newId")),
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
      result(4).conceptName shouldBe Some("Test")
      result(4).ohubId shouldBe Some("oldId")
    }
  }

}
