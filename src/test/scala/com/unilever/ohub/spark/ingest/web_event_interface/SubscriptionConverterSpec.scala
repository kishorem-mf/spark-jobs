package com.unilever.ohub.spark.ingest.web_event

import com.unilever.ohub.spark.domain.entity.{ ContactPerson, Subscription }
import com.unilever.ohub.spark.ingest.CsvDomainGateKeeperSpec
import org.apache.spark.sql.Dataset

class SubscriptionConverterSpec extends CsvDomainGateKeeperSpec[Subscription] {
  override val SUT = SubscriptionConverter

  describe("web event subscription converter") {
    it("should convert a subscription correctly from a valid web event csv input") {
      val inputFile = "src/test/resources/WEB_EVENT_SUBSCRIPTIONS.csv"

      runJobWith(inputFile) { actualDataSet: Dataset[Subscription] ⇒
        // placeholder for unit test
      }
    }
  }
}
