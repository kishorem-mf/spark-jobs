package com.unilever.ohub.spark.tsv2parquet.fuzzit

import com.unilever.ohub.spark.domain.entity.{ Product, TestProducts }
import com.unilever.ohub.spark.tsv2parquet.DomainGateKeeperCsvSpec

class ProductConverterSpec extends DomainGateKeeperCsvSpec[Product] with TestProducts {

  private[tsv2parquet] override val SUT = ProductConverter

  describe("fuzzit product converter") {
    it("should convert a product correctly from a valid fuzzit csv input") {
      val inputFile = "src/test/resources/FUZZIT_PRODUCTS.csv"

      runJobWith(inputFile) { actualDataSet ⇒
        actualDataSet.count() shouldBe 1

        val actualProduct = actualDataSet.head()
        val expectedProduct = defaultProductRecord.copy(
          concatId = s"1030~FUZZIT~00590126-4249-4EA5-B44D-6D52A8E8B86F",
          countryCode = "1030",
          isActive = true,
          isGoldenRecord = true,
          ohubId = actualProduct.ohubId,
          name = "ASPERGECREME SUPERIEURSOEP XL 2,7KG KNORR",
          sourceEntityId = "00590126-4249-4EA5-B44D-6D52A8E8B86F",
          sourceName = "FUZZIT",
          ohubCreated = actualProduct.ohubCreated,
          ohubUpdated = actualProduct.ohubUpdated,
          productId = actualProduct.productId,
          code = Some("8712566947799"),
          codeType = Some("MRDR"),
          eanDistributionUnit = Some("8712566947799")
        )

        actualProduct shouldBe expectedProduct
      }
    }
  }
}
