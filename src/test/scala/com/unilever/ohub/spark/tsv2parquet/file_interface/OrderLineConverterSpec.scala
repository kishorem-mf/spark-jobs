package com.unilever.ohub.spark.tsv2parquet.file_interface

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.{ OrderLine, TestOrderLines }
import com.unilever.ohub.spark.tsv2parquet.CsvDomainGateKeeperSpec
import com.unilever.ohub.spark.tsv2parquet.DomainGateKeeper.DomainConfig

class OrderLineConvertorSpec extends CsvDomainGateKeeperSpec[OrderLine] with TestOrderLines {

  private[tsv2parquet] override val SUT = OrderLineConverter

  describe("file interface product converter") {
    it("should convert a product correctly from a valid file interface csv input") {
      val inputFile = "src/test/resources/FILE_PRODUCTS.csv"
      val config = DomainConfig(inputFile = inputFile, outputFile = "", fieldSeparator = "‰")

      runJobWith(config) { actualDataSet ⇒
        actualDataSet.count() shouldBe 1

        val actualOrderLine = actualDataSet.head
        val expectedOrderLine = defaultOrderLine.copy(
          concatId = defaultOrderLine.concatId,
          countryCode = "AU",
          dateCreated = Some(Timestamp.valueOf("2015-06-30 13:47:00")),
          dateUpdated = Some(Timestamp.valueOf("2015-06-30 13:48:00")),
          isActive = true,
          isGoldenRecord = true,
          ohubId = actualOrderLine.ohubId,
          sourceEntityId = "P1234",
          sourceName = "WUFOO",
          ohubCreated = actualOrderLine.ohubCreated,
          ohubUpdated = actualOrderLine.ohubUpdated,
          orderConcatId = Some("AU~WUFOO~P1234"),
          productConcatId = Some("P1234"),
          quantityOfUnits = Some(6),
          amount = Some(10),
          pricePerUnit = Some(5),
          currency = Some("AUD")
        )

        actualOrderLine shouldBe expectedOrderLine
      }
    }
  }
}
