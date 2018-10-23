package com.unilever.ohub.spark.ingest.file_interface

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.{ Order, TestOrders }
import com.unilever.ohub.spark.ingest.{ CsvDomainConfig, CsvDomainGateKeeperSpec }

class OrderConverterSpec extends CsvDomainGateKeeperSpec[Order] with TestOrders {

  override val SUT = OrderConverter

  describe("file interface order converter") {
    it("should convert a order correctly from a valid file interface csv input") {
      val inputFile = "src/test/resources/FILE_ORDERS.csv"
      val config = CsvDomainConfig(inputFile = inputFile, outputFile = "", fieldSeparator = "‰")

      runJobWith(config) { actualDataSet ⇒
        actualDataSet.count() shouldBe 1

        val actualOrder = actualDataSet.head
        val expectedOrder = defaultOrder.copy(
          concatId = "AU~WUFOO~O1234",
          countryCode = "AU",
          dateCreated = Some(Timestamp.valueOf("2015-06-30 13:47:00")),
          dateUpdated = Some(Timestamp.valueOf("2015-06-30 13:48:00")),
          isActive = true,
          isGoldenRecord = true,
          ohubId = actualOrder.ohubId,
          sourceEntityId = "O1234",
          sourceName = "WUFOO",
          ohubCreated = actualOrder.ohubCreated,
          ohubUpdated = actualOrder.ohubUpdated,
          `type` = "UNKNOWN",
          campaignCode = Some("E1234"),
          campaignName = Some("Sample campaign"),
          contactPersonConcatId = Some("AB123"),
          distributorName = Some("SLIGRO"),
          operatorConcatId = "E1-1234",
          transactionDate = Timestamp.valueOf("2015-09-30 14:23:00")
        )

        actualOrder shouldBe expectedOrder
      }
    }
  }
}
