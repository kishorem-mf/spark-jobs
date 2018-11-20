package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.TestProducts
import org.apache.spark.sql.Dataset
import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.domain.entity.Product

class ProductMergingSpec extends SparkJobSpec with TestProducts {

  import spark.implicits._

  private val SUT = ProductMerging

  describe("product merging") {
    it("a new product should get an ohubId and be marked golden record") {
      val newRecord = defaultProduct.copy(
        concatId = "new",
        isGoldenRecord = false,
        ohubId = None
      )

      val delta = Seq(
        newRecord
      ).toDataset

      val integrated = Seq[Product]().toDataset

      val result = SUT.transform(spark, delta, integrated).collect()

      result.size shouldBe 1
      result.head.ohubId shouldBe defined
      result.head.isGoldenRecord shouldBe true
    }

    it("should take newest data if available while retaining ohubId") {
      val updatedRecord = defaultProduct.copy(
        isGoldenRecord = true,
        ohubId = Some("oldId"),
        countryCode = "updated",
        concatId = s"updated~${defaultProduct.sourceName}~${defaultProduct.sourceEntityId}",
        name = "Calve")

      val deletedRecord = defaultProduct.copy(
        isGoldenRecord = true,
        countryCode = "deleted",
        concatId = s"deleted~${defaultProduct.sourceName}~${defaultProduct.sourceEntityId}",
        isActive = true)

      val newRecord = defaultProduct.copy(
        isGoldenRecord = true,
        countryCode = "new",
        concatId = s"new~${defaultProduct.sourceName}~${defaultProduct.sourceEntityId}"
      )

      val unchangedRecord = defaultProduct.copy(
        isGoldenRecord = true,
        countryCode = "unchanged",
        concatId = s"unchanged~${defaultProduct.sourceName}~${defaultProduct.sourceEntityId}"
      )

      val notADeltaRecord = defaultProduct.copy(
        isGoldenRecord = true,
        countryCode = "notADelta",
        concatId = s"notADelta~${defaultProduct.sourceName}~${defaultProduct.sourceEntityId}"
      )

      val previous: Dataset[Product] = spark.createDataset(Seq(
        updatedRecord,
        deletedRecord,
        unchangedRecord,
        notADeltaRecord
      ))
      val input: Dataset[Product] = spark.createDataset(Seq(
        updatedRecord.copy(name = "Unox", ohubId = Some("newId")),
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
      result(4).name shouldBe "Unox"
      result(4).ohubId shouldBe Some("oldId")
    }
  }
}
