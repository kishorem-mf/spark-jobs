package com.unilever.ohub.spark.export.ddl

import java.sql.Timestamp

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.{TestOperators, TestOrders}
import com.unilever.ohub.spark.export.ddl.model.DdlOrder

class OrderDdlConverterSpec extends SparkJobSpec with TestOrders with TestOperators {
  val SUT = OrderDdlConverter
  val orderToConvert = defaultOrder.copy(ohubId = Some("12345"),comment = Some("comment"),  dateCreated = Some(Timestamp.valueOf("2021-02-12 11:11:11")))

  describe("Order ddl converter") {
    it("should convert a order parquet correctly into an order csv") {
      val result = SUT.convert(orderToConvert)
      val expectedDdlOrder = DdlOrder(
        createdDate = "2021-02-12 11:11:11:000",
        accountId = "id",
        createdBySAP = "",
        purchaseOrderType = "",
        niv = "",
        totalGrossPrice = "",
        totalSurcharge = "",
        discount = "",
        rejectionStatus = "",
        deliveryStatus = "",
        amount = "10.00",
        orderName = "comment"
      )
      result shouldBe expectedDdlOrder
    }
  }
}
