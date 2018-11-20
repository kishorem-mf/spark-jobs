package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.{ OrderLine, TestOrderLines }
import com.unilever.ohub.spark.ingest.{ CsvDomainConfig, CsvDomainGateKeeperSpec }

class OrderLineConverterSpec extends CsvDomainGateKeeperSpec[OrderLine] with TestOrderLines {

  override val SUT = OrderLineConverter

  describe("common order line converter") {
    it("should convert an order line correctly from a valid csv input") {
      val inputFile = "src/test/resources/COMMON_ORDERS.csv"
      val config = CsvDomainConfig(inputFile = inputFile, outputFile = "", fieldSeparator = ";")

      runJobWith(config) { actualDataSet ⇒
        actualDataSet.count() shouldBe 1

        val actualOrderLine = actualDataSet.head
        val expectedOrderLine = defaultOrderLine.copy(
          id = "id-1",
          creationTimestamp = new Timestamp(1542205922011L),
          concatId = "AU~WUFOO~O1234",
          countryCode = "AU",
          dateCreated = Some(Timestamp.valueOf("2015-06-30 13:47:00")),
          dateUpdated = Some(Timestamp.valueOf("2015-06-30 13:48:00")),
          isActive = true,
          isGoldenRecord = false,
          ohubId = actualOrderLine.ohubId,
          sourceEntityId = "O1234",
          sourceName = "WUFOO",
          ohubCreated = actualOrderLine.ohubCreated,
          ohubUpdated = actualOrderLine.ohubUpdated,
          orderConcatId = "AU~WUFOO~O1234",
          productConcatId = "AU~WUFOO~P1234",
          productSourceEntityId = "P1234",
          quantityOfUnits = 6,
          amount = BigDecimal(10),
          pricePerUnit = Some(BigDecimal(5)),
          currency = Some("AUD"),
          comment = Some("comment"),
          campaignLabel = Some("label"),
          loyaltyPoints = Some(25),
          productOhubId = None
        )

        actualOrderLine shouldBe expectedOrderLine
      }
    }
  }
}
