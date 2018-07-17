package com.unilever.ohub.spark.ingest.web_event_interface

import com.unilever.ohub.spark.domain.entity.Order
import com.unilever.ohub.spark.ingest.CsvDomainGateKeeperSpec
import org.apache.spark.sql.Dataset

class OrderConverterSpec extends CsvDomainGateKeeperSpec[Order] {
  override val SUT = OrderConverter

  describe("web event order converter") {
    it("should convert an order correctly from a valid web event csv input") {
      val inputFile = "src/test/resources/WEB_EVENT_ORDERS.csv"

      runJobWith(inputFile) { actualDataSet: Dataset[Order] ⇒
        // placeholder for unit test

        println(actualDataSet)
      }
    }
  }
}
