package com.unilever.ohub.spark.acm

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.acm.model.AcmProduct
import org.apache.spark.sql.Dataset
import com.unilever.ohub.spark.domain.entity.{ Product, TestProducts }

class ProductAcmConverterSpec extends SparkJobSpec with TestProducts {

  private[acm] val SUT = ProductAcmConverter

  describe("product acm delta converter") {
    it("should convert a domain product correctly into an acm converter containing only delta records") {
      import spark.implicits._

      val updatedRecord = defaultProduct.copy(
        isGoldenRecord = true,
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
        updatedRecord.copy(name = "Unox"),
        deletedRecord.copy(isActive = false),
        unchangedRecord,
        newRecord
      ))

      val result = SUT.transform(spark, input, previous)
        .collect()
        .sortBy(_.COUNTRY_CODE)

      result.length shouldBe 3
      assert(result.head.COUNTRY_CODE == Some("deleted"))
      assert(result.head.DELETE_FLAG == Some("Y"))
      assert(result(1).COUNTRY_CODE == Some("new"))
      assert(result(2).COUNTRY_CODE == Some("updated"))
      assert(result(2).PRODUCT_NAME.contains("Unox"))
    }
  }

  describe("product acm converter") {
    it("should convert a domain Product correctly into an acm AcmProduct") {
      import spark.implicits._

      val input: Dataset[Product] = spark.createDataset(Seq(defaultProduct.copy(isGoldenRecord = true)))
      val result = SUT.transform(spark, input, spark.emptyDataset[Product])

      result.count() shouldBe 1

      val actualAcmProduct = result.head()
      val expectedAcmProduct = AcmProduct(
        COUNTRY_CODE = Some("country-code"),
        PRODUCT_NAME = Some("product-name"),
        PRD_INTEGRATION_ID = "country-code~source-name~source-entity-id",
        EAN_CODE = None,
        MRDR_CODE = None,
        CREATED_AT = None,
        UPDATED_AT = None,
        DELETE_FLAG = Some("N")
      )

      actualAcmProduct shouldBe expectedAcmProduct
    }
  }
}
