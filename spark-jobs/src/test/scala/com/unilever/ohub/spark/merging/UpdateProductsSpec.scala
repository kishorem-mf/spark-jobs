package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.TestProducts
import com.unilever.ohub.spark.SharedSparkSession.spark

class UpdateProductsSpec extends SparkJobSpec with TestProducts {

  import spark.implicits._

  private val SUT = UpdateProducts

  describe("product updating") {
    it("update products with sifu products based on product code") {
      val newSifuRecord = defaultProductSifu.copy(
        productCode = Some("A123"),
        url = Some("abc"),
        brandName = Some("aaa"),
        country = Some("nl")
      )

      val sifuProducts = Seq(
        newSifuRecord
      ).toDataset

      val integrated = defaultProduct.copy(
        isGoldenRecord = true,
        ohubId = Some("1234"),
        concatId = "AU~WUFOO~1234",
        code = Some("A123"),
        url = Some("ccc"),
        brandName = Some("bbb"),
        countryCode = "NL"
      )

      val integratedProducts = Seq(
        integrated
      ).toDataset

      val result = SUT.transform(spark, sifuProducts, integratedProducts).collect()

      result.size shouldBe 1
      val prodResult = result.head
      prodResult.url shouldBe Some("abc")
    }
    it("update products with sifu products based on product number") {
      val newSifuRecord = defaultProductSifu.copy(
        productNumber=Some("A123"),
        url = Some("abc"),
        brandName = Some("aaa"),
        country  = Some("nl")
      )

      val sifuProducts = Seq(
        newSifuRecord
      ).toDataset

      val integrated = defaultProduct.copy(
        isGoldenRecord = true,
        ohubId = Some("1234"),
        concatId = "AU~WUFOO~1234",
        code=Some("A123"),
        url = Some("ccc"),
        brandName = Some("bbb"),
        countryCode  = "NL"
      )

      val integratedProducts = Seq(
        integrated
      ).toDataset


      val result = SUT.transform(spark,sifuProducts, integratedProducts).collect()

      result.size shouldBe 1
      val prodResult = result.head
      prodResult.url shouldBe Some("abc")
    }
  }
}
