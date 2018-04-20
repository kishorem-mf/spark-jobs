package com.unilever.ohub.spark.acm

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.acm.model.UFSProduct
import org.apache.spark.sql.Dataset
import com.unilever.ohub.spark.domain.entity.{ Product, TestProducts }

class ProductAcmConverterSpec extends SparkJobSpec with TestProducts {

  private[acm] val SUT = ProductAcmConverter

  describe("product acm converter") {
    it("should convert a domain Product correctly into an acm UFSProduct") {
      import spark.implicits._

      val input: Dataset[Product] = spark.createDataset(Seq(defaultProductRecord.copy(isGoldenRecord = true)))
      val result = SUT.transform(spark, input)

      result.count() shouldBe 1

      val actualUFSProduct = result.head()
      val expectedUFSProduct = UFSProduct(
        COUNTY_CODE = Some("country-code"),
        PRODUCT_NAME = Some("product-name"),
        PRD_INTEGRATION_ID = "country-code~source-name~source-entity-id",
        EAN_CODE = None,
        MRDR_CODE = None,
        CREATED_AT = None,
        UPDATED_AT = None,
        DELETE_FLAG = Some("N")
      )

      actualUFSProduct shouldBe expectedUFSProduct
    }
  }
}
