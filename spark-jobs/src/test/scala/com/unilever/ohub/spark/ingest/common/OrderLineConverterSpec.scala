package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.{ OrderLine, TestOrderLines }
import com.unilever.ohub.spark.ingest.{ CsvDomainConfig, CsvDomainGateKeeperSpec }

class OrderLineConverterSpec extends CsvDomainGateKeeperSpec[OrderLine] with TestOrderLines {

  override val SUT = OrderLineConverter

  describe("common order line converter") {
    it("should convert an order line correctly from a valid csv input") {
      val inputFile = "src/test/resources/COMMON_ORDERLINES.csv"
      val config = CsvDomainConfig(inputFile = inputFile, outputFile = "", fieldSeparator = ";")

      runJobWith(config) { actualDataSet ⇒
        actualDataSet.count() shouldBe 1

        val actualOrderLine = actualDataSet.head
        val expectedOrderLine = OrderLine(
          id = "084dd5d1-b088-4a27-a1aa-9af27f8e5d5d",
          creationTimestamp = new Timestamp(1545151257406L),
          concatId = "NZ~EMAKINA~1~E627934C019D7C338C68928C8CB1B78F24275097~20160420152727",
          countryCode = "NZ",
          customerType = OrderLine.customerType,
          dateCreated = Some(Timestamp.valueOf("2016-04-25 13:23:48")),
          dateUpdated = Some(Timestamp.valueOf("2016-04-25 13:23:48")),
          isActive = true,
          isGoldenRecord = false,
          ohubId = None, // set in OrderLineMerging
          sourceEntityId = "1~E627934C019D7C338C68928C8CB1B78F24275097~20160420152727",
          sourceName = "EMAKINA",
          ohubCreated = actualOrderLine.ohubCreated,
          ohubUpdated = actualOrderLine.ohubUpdated,
          // specific fields
          orderConcatId = "NZ~EMAKINA~E627934C019D7C338C68928C8CB1B78F24275097~20160420152727",
          productConcatId = "NZ~EMAKINA~68024380",
          productSourceEntityId = "68024380",
          comment = None,
          quantityOfUnits = 1,
          amount = BigDecimal(0),
          pricePerUnit = Some(BigDecimal(0)),
          currency = None,
          campaignLabel = None,
          loyaltyPoints = None,
          productOhubId = None, // set in OrderLineMerging
          orderType = Some("Merchandise"),
          // other fields
          additionalFields = Map(),
          ingestionErrors = Map()
        )
        actualOrderLine shouldBe expectedOrderLine
      }
    }
  }
}
