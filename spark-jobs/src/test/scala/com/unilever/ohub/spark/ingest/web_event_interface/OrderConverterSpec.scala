package com.unilever.ohub.spark.ingest.web_event_interface

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.{ Order, TestOrders }
import com.unilever.ohub.spark.ingest.CsvDomainGateKeeperSpec
import org.apache.spark.sql.Dataset
import cats.syntax.option.none

class OrderConverterSpec extends CsvDomainGateKeeperSpec[Order] with TestOrders {
  override val SUT = OrderConverter

  describe("web event order converter") {
    it("should convert an order correctly from a valid web event csv input") {
      val inputFile = "src/test/resources/WEB_EVENT_ORDERS.csv"

      runJobWith(inputFile) { actualDataSet: Dataset[Order] ⇒
        actualDataSet.count() shouldBe 1

        val actualOrder: Order = actualDataSet.head()
        val expectedOrder = Order(
          concatId = "NL~EMAKINA~MyRefOrderId",
          countryCode = "NL",
          customerType = "ORDER",
          dateCreated = none,
          dateUpdated = none,
          isActive = true,
          isGoldenRecord = false,
          sourceEntityId = "MyRefOrderId",
          sourceName = "EMAKINA",
          ohubId = none,
          ohubCreated = actualOrder.ohubCreated,
          ohubUpdated = actualOrder.ohubUpdated,
          `type` = "EVENT",
          campaignCode = "MyCampaignCode",
          campaignName = "MyCampaignName",
          comment = "MyComments",
          contactPersonConcatId = none,
          contactPersonOhubId = none,
          distributorId = "wholesaler-id",
          distributorLocation = "wholesaler-location",
          distributorName = "wholesaler",
          distributorOperatorId = "wholesaler-customer-number",
          operatorConcatId = "NL~EMAKINA~MyRefOrderId",
          operatorOhubId = none,
          transactionDate = Timestamp.valueOf("2018-07-17 09:30:20.0"),
          vat = BigDecimal(0),
          invoiceOperatorName = "MyName",
          invoiceOperatorStreet = "MyStreet",
          invoiceOperatorHouseNumber = "MyHouseNumber",
          invoiceOperatorHouseNumberExtension = "MyHouseNumberExtension",
          invoiceOperatorZipCode = "MyPostCode",
          invoiceOperatorCity = "MyCity",
          invoiceOperatorState = "MyState",
          invoiceOperatorCountry = "MyCountry",
          deliveryOperatorName = "MyName",
          deliveryOperatorStreet = "MyStreet",
          deliveryOperatorHouseNumber = "MyHouseNumber",
          deliveryOperatorHouseNumberExtension = "MyHouseNumberExtension",
          deliveryOperatorZipCode = "MyPostCode",
          deliveryOperatorCity = "MyCity",
          deliveryOperatorState = "MyState",
          deliveryOperatorCountry = "MyCountry",
          additionalFields = Map.empty,
          ingestionErrors = Map.empty
        )

        actualOrder shouldEqual expectedOrder
      }
    }
  }
}
