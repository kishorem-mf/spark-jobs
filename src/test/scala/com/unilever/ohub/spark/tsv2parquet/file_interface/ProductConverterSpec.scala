package com.unilever.ohub.spark.tsv2parquet.file_interface

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.{ Product, TestProducts }
import com.unilever.ohub.spark.tsv2parquet.CsvDomainGateKeeperSpec
import com.unilever.ohub.spark.tsv2parquet.DomainGateKeeper.DomainConfig

class ProductConverterSpec extends CsvDomainGateKeeperSpec[Product] with TestProducts {

  private[tsv2parquet] override val SUT = ProductConverter

  describe("file interface product converter") {
    it("should convert a product correctly from a valid file interface csv input") {
      val inputFile = "src/test/resources/FILE_PRODUCTS.csv"
      val config = DomainConfig(inputFile = inputFile, outputFile = "", fieldSeparator = "‰")

      runJobWith(config) { actualDataSet ⇒
        actualDataSet.count() shouldBe 1

        val actualProduct = actualDataSet.head()
        val expectedProduct = defaultProduct.copy(
          concatId = "AU~WUFOO~P1234",
          countryCode = "AU",
          dateCreated = Some(Timestamp.valueOf("2015-06-30 13:47:00")),
          dateUpdated = Some(Timestamp.valueOf("2015-06-30 13:48:00")),
          isActive = true,
          isGoldenRecord = true,
          ohubId = actualProduct.ohubId,
          name = "KNORR CHICKEN POWDER(D) 12X1kg",
          sourceEntityId = "P1234",
          sourceName = "WUFOO",
          ohubCreated = actualProduct.ohubCreated,
          ohubUpdated = actualProduct.ohubUpdated,
          code = Some("201119"),
          codeType = Some("MRDR"),
          currency = Some("GBP"),
          eanConsumerUnit = Some("812234000000"),
          eanDistributionUnit = Some("112234000000"),
          productId = actualProduct.productId,
          `type` = Some("Product"),
          unit = Some("Cases"),
          unitPrice = Some(BigDecimal(4))
        )

        actualProduct shouldBe expectedProduct
      }
    }
  }
}
