package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.Activity
import com.unilever.ohub.spark.ingest.CsvDomainGateKeeperSpec

class ActivityConverterSpec extends CsvDomainGateKeeperSpec[Activity] {

  override val SUT = ActivityConverter

  describe("common subscription converter") {
    it("should convert a subscription correctly from a valid common csv input") {
      val inputFile = "src/test/resources/COMMON_ACTIVITY.csv"

      runJobWith(inputFile) { actualDataSet ⇒
        actualDataSet.count() shouldBe 1

        val actualActivity = actualDataSet.head()

        val expectedActivity = Activity(
          concatId = "DE~EMAKINA~123",
          countryCode = "DE",
          customerType = "CONTACTPERSON",
          sourceEntityId = "123",
          sourceName = "EMAKINA",
          isActive = true,
          ohubCreated = actualActivity.ohubCreated,
          ohubUpdated = actualActivity.ohubUpdated,
          dateCreated = Some(Timestamp.valueOf("2017-12-18 10:33:54.0")),
          dateUpdated = Some(Timestamp.valueOf("2017-12-18 10:33:54.0")),
          ohubId = Option.empty,
          isGoldenRecord = false,

          activityDate = Some(Timestamp.valueOf("2017-12-18 10:33:54.0")),
          name = Some("QA"),
          details = Some("answering questions"),
          actionType = Some("type"),
          contactPersonConcatId = Some("DE~EMAKINA~456"),
          contactPersonOhubId = None,

          additionalFields = Map(),
          ingestionErrors = Map()
        )
        actualActivity shouldBe expectedActivity
      }
    }
  }
}
