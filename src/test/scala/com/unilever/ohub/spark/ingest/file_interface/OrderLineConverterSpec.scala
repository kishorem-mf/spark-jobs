package com.unilever.ohub.spark.ingest.file_interface

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.{ OrderLine, TestOrderLines }
import com.unilever.ohub.spark.ingest.CsvDomainGateKeeperSpec
import com.unilever.ohub.spark.ingest.DomainGateKeeper.DomainConfig

class OrderLineConverterSpec extends CsvDomainGateKeeperSpec[OrderLine] with TestOrderLines {

  override val SUT = OrderLineConverter

  describe("file interface order line converter") {
    it("should convert an order line correctly from a valid file interface csv input") {
      val inputFile = "src/test/resources/FILE_ORDERS.csv"
      val config = DomainConfig(inputFile = inputFile, outputFile = "", fieldSeparator = "‰")

      runJobWith(config) { actualDataSet ⇒
        actualDataSet.count() shouldBe 1

        val actualOrderLine = actualDataSet.head
        val expectedOrderLine = defaultOrderLine.copy(
          concatId = "AU~WUFOO~O1234_P1234",
          countryCode = "AU",
          dateCreated = Some(Timestamp.valueOf("2015-06-30 13:47:00")),
          dateUpdated = Some(Timestamp.valueOf("2015-06-30 13:48:00")),
          isActive = true,
          isGoldenRecord = true,
          ohubId = actualOrderLine.ohubId,
          sourceEntityId = "O1234_P1234",
          sourceName = "WUFOO",
          ohubCreated = actualOrderLine.ohubCreated,
          ohubUpdated = actualOrderLine.ohubUpdated,
          orderConcatId = "AU~WUFOO~O1234",
          productConcatId = "AU~WUFOO~P1234",
          quantityOfUnits = 6L,
          amount = BigDecimal(10),
          pricePerUnit = Some(BigDecimal(5)),
          currency = Some("AUD"),
          campaignLabel = None,
          loyaltyPoints = None,
          productOhubId = None
        )

        actualOrderLine shouldBe expectedOrderLine
      }
    }
  }
}
