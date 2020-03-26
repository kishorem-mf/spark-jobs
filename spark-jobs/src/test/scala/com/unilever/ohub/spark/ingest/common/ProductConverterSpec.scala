package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.{ Product, TestProducts }
import com.unilever.ohub.spark.ingest.{ CsvDomainConfig, CsvDomainGateKeeperSpec }

class ProductConverterSpec extends CsvDomainGateKeeperSpec[Product] with TestProducts {

  override val SUT = ProductConverter

  describe("file interface product converter") {
    it("should convert a product correctly from a valid file interface csv input") {
      val inputFile = "src/test/resources/COMMON_PRODUCTS.csv"
      val config = CsvDomainConfig(inputFile = inputFile, outputFile = "", fieldSeparator = ";")

      runJobWith(config) { actualDataSet ⇒
        actualDataSet.count() shouldBe 1

        val actualProduct = actualDataSet.head()
        val expectedProduct = defaultProduct.copy(
          id = "id-1",
          creationTimestamp = new Timestamp(1542205922011L),
          concatId = "AU~WUFOO~P1234",
          countryCode = "AU",
          dateCreated = Some(Timestamp.valueOf("2015-06-30 13:47:00")),
          dateUpdated = Some(Timestamp.valueOf("2015-06-30 13:48:00")),
          isActive = true,
          isGoldenRecord = false,
          ohubId = None,
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
          `type` = Some("Loyalty"),
          unit = Some("Cases"),
          unitPrice = Some(BigDecimal(4)),
          brandCode = Some("brand"),
          subBrandCode = Some("subBrand")
        )

        actualProduct shouldBe expectedProduct
      }
    }
  }
}
