package com.unilever.ohub.spark.export.acm

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.TestActivities
import com.unilever.ohub.spark.export.acm.model.AcmActivity
import org.scalatest.{FunSpec, Matchers}

class ActivityAcmConverterSpec extends FunSpec with TestActivities with Matchers {

  private[acm] val SUT = ActivityAcmConverter

  describe("Activity acm converter") {
    it("should convert a activity correctly into an acm activity") {
      val activity = defaultActivity.copy(dateCreated = Some(Timestamp.valueOf("2015-06-30 13:47:00.0")), dateUpdated = Some(Timestamp.valueOf("2015-06-30 13:47:00.0")))
      val result = SUT.convert(activity)

      val expectedAcmActivity = AcmActivity(
        ACTIVITY_ID = "DE~EMAKINA~b3a6208c-d7f6-44e2-80e2-f26d461f64c0",
        COUNTRY_CODE = "DE",
        CP_ORIG_INTEGRATION_ID = "",
        DELETE_FLAG = "N",
        DATE_CREATED = "2015/06/30 13:47:00",
        DATE_UPDATED = "2015/06/30 13:47:00",
        DETAILS = "some details",
        TYPE = "MyActionType",
        NAME = "some name")

      result shouldBe expectedAcmActivity
    }
  }
}
