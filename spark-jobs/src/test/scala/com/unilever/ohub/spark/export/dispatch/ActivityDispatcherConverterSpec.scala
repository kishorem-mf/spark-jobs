package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.TestActivities
import com.unilever.ohub.spark.export.dispatch.model.DispatchActivity

class ActivityDispatcherConverterSpec extends SparkJobSpec with TestActivities {

  val SUT = ActivityDispatcherConverter

  describe("Activity dispatch converter") {
    it("should convert an activity into an dispatch activity") {
      val result = SUT.convert(defaultActivity)

      val expectedActivity = DispatchActivity(
        CP_ORIG_INTEGRATION_ID= "DE~SUBSCRIPTION~138175",
        RAC_INTEGRATION_ID= "DE~EMAKINA~b3a6208c-d7f6-44e2-80e2-f26d461f64c0",
        SOURCE= "EMAKINA",
        COUNTRY_CODE= "DE",
        CREATED_AT= "2015-06-30 13:49:00",
        UPDATED_AT= "2015-06-30 13:49:00",
        DELETE_FLAG= "N",
        CONTACT_DATE= "",
        ACTION_TYPE= "MyActionType",
        ACTIVITY_NAME= "some name",
        ACTIVITY_DETAILS= "some details"
      )

      result shouldBe expectedActivity
    }
  }
}
