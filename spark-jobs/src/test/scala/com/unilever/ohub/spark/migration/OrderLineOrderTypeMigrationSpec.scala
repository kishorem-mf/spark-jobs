package com.unilever.ohub.spark.migration

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity._

class OrderLineOrderTypeMigrationSpec extends SparkJobSpec with TestOrderLines with TestOrders {

  import spark.implicits._

  private val SUT = OrderLineOrderTypeMigration

  describe("OrderLineOrderTypeMigration") {
    it("should update orderType in ordeline according to matching order") {
      val orderLine1 = defaultOrderLine.copy(orderConcatId = "123", orderType = None)
      val orderLine2 = defaultOrderLine.copy(orderConcatId = "123", orderType = None)
      val orderLine3 = defaultOrderLine.copy(orderConcatId = "789", orderType = None)

      val nonMatchingOrder = defaultOrder.copy(concatId = "456", `type` = "NOT_SSD")
      val matchingOrder = defaultOrder.copy(concatId = "123", `type` = "SSD")
      val otherMatchingOrder = defaultOrder.copy(concatId = "789", `type` = "SSD_ALT")

      val orderLines = Seq(orderLine1, orderLine2, orderLine3).toDataset
      val orders = Seq(matchingOrder, nonMatchingOrder, otherMatchingOrder).toDataset

      val result = SUT.transform(spark, orderLines, orders).collect().sortBy(_.orderConcatId)

      result.length shouldBe 3
      result(0).orderType shouldBe Some("SSD")
      result(1).orderType shouldBe Some("SSD")
      result(2).orderType shouldBe Some("SSD_ALT")
    }

    it("should leave unmatched orderlines as is") {
      val orderLine = defaultOrderLine.copy(orderConcatId = "123", orderType = None)
      val order = defaultOrder.copy(concatId = "456", `type` = "SSD")

      val orderLines = Seq(orderLine).toDataset
      val orders = Seq(order).toDataset

      val result = SUT.transform(spark, orderLines, orders).collect()

      result.length shouldBe 1
      result(0).orderType shouldBe None
    }
  }
}
