package com.unilever.ohub.spark.acm

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.acm.model.UFSOrder
import org.apache.spark.sql.Dataset
import com.unilever.ohub.spark.domain.entity.{ Order, OrderLine, TestOrders }

class OrderAcmConverterSpec extends SparkJobSpec with TestOrders {

  private[acm] val SUT = OrderAcmConverter

  describe("Order acm delta converter") {
    it("should convert a domain Order correctly into an acm converter containing only delta records") {
      import spark.implicits._

      val updatedRecord = defaultOrder.copy(
        isGoldenRecord = true,
        countryCode = "updated",
        concatId = s"updated~${defaultOrder.sourceName}~${defaultOrder.sourceEntityId}")

      val deletedRecord = defaultOrder.copy(
        isGoldenRecord = true,
        countryCode = "deleted",
        concatId = s"deleted~${defaultOrder.sourceName}~${defaultOrder.sourceEntityId}",
        isActive = true)

      val newRecord = defaultOrder.copy(
        isGoldenRecord = true,
        countryCode = "new",
        concatId = s"new~${defaultOrder.sourceName}~${defaultOrder.sourceEntityId}"
      )

      val unchangedRecord = defaultOrder.copy(
        isGoldenRecord = true,
        countryCode = "unchanged",
        concatId = s"unchanged~${defaultOrder.sourceName}~${defaultOrder.sourceEntityId}"
      )

      val notADeltaRecord = defaultOrder.copy(
        isGoldenRecord = true,
        countryCode = "notADelta",
        concatId = s"notADelta~${defaultOrder.sourceName}~${defaultOrder.sourceEntityId}"
      )

      val previous: Dataset[Order] = spark.createDataset(Seq(
        updatedRecord,
        deletedRecord,
        unchangedRecord,
        notADeltaRecord
      ))
      val input: Dataset[Order] = spark.createDataset(Seq(
        updatedRecord.copy(countryCode = "updaaated"),
        deletedRecord.copy(isActive = false),
        unchangedRecord,
        newRecord
      ))

      val orderLines = spark.emptyDataset[OrderLine]

      val result = SUT.transform(spark, input, previous, orderLines)
        .collect()
        .sortBy(_.COUNTRY_CODE)

      result.length shouldBe 3
      // assert(result.head.COUNTRY_CODE == Some("deleted"))
      // assert(result.head.DELETE_FLAG == Some("Y"))
      // assert(result(1).COUNTRY_CODE == Some("new"))
      // assert(result(2).COUNTRY_CODE == Some("updated"))
      // assert(result(2).Order_NAME.contains("Unox"))
    }
  }

  describe("Order acm converter") {
    it("should convert a domain Order correctly into an acm UFSOrder") {
      import spark.implicits._

      val input: Dataset[Order] = spark.createDataset(Seq(defaultOrder.copy(isGoldenRecord = true)))
      val result = SUT.transform(spark, input, spark.emptyDataset[Order], spark.emptyDataset[OrderLine])

      result.count() shouldBe 1

      val actualUFSOrder = result.head()
      val expectedUFSOrder = UFSOrder(
        ORDER_ID = "",
        REF_ORDER_ID = None,
        COUNTRY_CODE = "",
        ORDER_TYPE = "",
        CP_LNKD_INTEGRATION_ID = None,
        OPR_LNKD_INTEGRATION_ID = "",
        CAMPAIGN_CODE = None,
        CAMPAIGN_NAME = None,
        WHOLESALER = None,
        WHOLESALER_ID = None,
        WHOLESALER_CUSTOMER_NUMBER = None,
        WHOLESALER_LOCATION = None,
        ORDER_TOKEN = None,
        ORDER_EMAIL_ADDRESS = None,
        ORDER_PHONE_NUMBER = None,
        ORDER_MOBILE_PHONE_NUMBER = None,
        TRANSACTION_DATE = "",
        ORDER_AMOUNT = 0.0,
        ORDER_AMOUNT_CURRENCY_CODE = "",
        DELIVERY_STREET = "",
        DELIVERY_HOUSENUMBER = "",
        DELIVERY_ZIPCODE = "",
        DELIVERY_CITY = "",
        DELIVERY_STATE = "",
        DELIVERY_COUNTRY = "",
        DELIVERY_PHONE = "",
        INVOICE_NAME = None,
        INVOICE_STREET = None,
        INVOICE_HOUSE_NUMBER = None,
        INVOICE_HOUSE_NUMBER_EXT = None,
        INVOICE_ZIPCODE = None,
        INVOICE_CITY = None,
        INVOICE_STATE = None,
        INVOICE_COUNTRY = None,
        COMMENTS = None,
        VAT = None,
        DELETED_FLAG = "N"
      )

      actualUFSOrder shouldBe expectedUFSOrder
    }
  }
}
