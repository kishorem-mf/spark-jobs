package com.unilever.ohub.spark.tsv2parquet.sifu

import com.unilever.ohub.spark.domain.entity.{Product, TestProducts}
import com.unilever.ohub.spark.tsv2parquet.DomainGateKeeperDatasetSpec

class ProductConverterSpec  extends DomainGateKeeperDatasetSpec[Product] with TestProducts {

  private[tsv2parquet] override val SUT = ProductConverter

  describe("sifu product converter") {
    it("should convert a product correctly from a valid api input") {

      runJobWith(Seq(TestProducts.sifuProductResponse)) { actualDataSet ⇒
        actualDataSet.count() shouldBe 1

        val actualProduct = actualDataSet.head()
        val expectedProduct = defaultProductRecord.copy()

        actualProduct shouldBe expectedProduct
      }
    }
  }
}
