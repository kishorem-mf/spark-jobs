package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.{ Order, TestOrders }
import com.unilever.ohub.spark.ingest.{ CsvDomainConfig, CsvDomainGateKeeperSpec }

class OrderConverterSpec extends CsvDomainGateKeeperSpec[Order] with TestOrders {

  override val SUT = OrderConverter

  describe("common order converter") {
    it("should convert a order correctly from a valid csv input") {
      val inputFile = "src/test/resources/COMMON_ORDERS.csv" // TODO what about ORDER_VALUE?
      val config = CsvDomainConfig(inputFile = inputFile, outputFile = "", fieldSeparator = ";")

      runJobWith(config) { actualDataSet ⇒
        actualDataSet.count() shouldBe 1

        val actualOrder = actualDataSet.head
        val expectedOrder = defaultOrder.copy(
          id = "id-1",
          creationTimestamp = new Timestamp(1542205922011L),
          concatId = "AU~WUFOO~O1234",
          countryCode = "AU",
          dateCreated = Some(Timestamp.valueOf("2015-06-30 13:47:00")),
          dateUpdated = Some(Timestamp.valueOf("2015-06-30 13:48:00")),
          isActive = true,
          isGoldenRecord = false,
          ohubId = actualOrder.ohubId,
          sourceEntityId = "O1234",
          sourceName = "WUFOO",
          ohubCreated = actualOrder.ohubCreated,
          ohubUpdated = actualOrder.ohubUpdated,
          orderUid = None,
          `type` = "UNKNOWN",
          campaignCode = Some("E1234"),
          campaignName = Some("Sample campaign"),
          contactPersonConcatId = Some("AB123"),
          comment = Some("comment"),
          distributorName = Some("SLIGRO"),
          distributorId = Some("dist1"),
          distributorLocation = Some("locA"),
          distributorOperatorId = Some("oper2"),
          currency = Some("AUD"),
          vat = Some(BigDecimal("12345")),
          amount = Some(BigDecimal("920")),
          operatorConcatId = Some("E1-1234"),
          transactionDate = Some(Timestamp.valueOf("2015-09-30 14:23:00"))
        )

        actualOrder shouldBe expectedOrder
      }
    }
  }
}
