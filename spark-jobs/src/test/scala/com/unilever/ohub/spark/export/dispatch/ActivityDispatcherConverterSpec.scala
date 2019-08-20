package com.unilever.ohub.spark.export.dispatch

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.TestActivities
import com.unilever.ohub.spark.export.dispatch.model.DispatchActivity

class ActivityDispatcherConverterSpec extends SparkJobSpec with TestActivities {

  val SUT = ActivityDispatcherConverter

  describe("Activity dispatch converter") {
    it("should convert an activity into an dispatch activity") {
      val result = SUT.convert(defaultActivity)

      val expectedActivity = DispatchActivity(
        CP_ORIG_INTEGRATION_ID = "DE~SUBSCRIPTION~138175",
        RAC_INTEGRATION_ID = "DE~EMAKINA~b3a6208c-d7f6-44e2-80e2-f26d461f64c0",
        SOURCE = "EMAKINA",
        COUNTRY_CODE = "DE",
        CREATED_AT = "2015-06-30 13:49:00",
        UPDATED_AT = "2015-06-30 13:49:00",
        DELETE_FLAG = "N",
        CONTACT_DATE = "",
        ACTION_TYPE = "MyActionType",
        ACTIVITY_NAME = "some name",
        ACTIVITY_DETAILS = "some details"
      )

      result shouldBe expectedActivity
    }

    it("should be able to explain the origin of the fields") {
      val result = SUT.convert(defaultActivity, true)

      val exampleDateTime = DateTimeFormatter.ofPattern(SUT.timestampPattern).format(LocalDateTime.now().truncatedTo(ChronoUnit.HOURS))

      val expectedActivity = DispatchActivity(
        CP_ORIG_INTEGRATION_ID = "{\"fromEntity\":\"Activity\",\"fromFieldName\":\"contactPersonConcatId\",\"fromFieldType\":\"String\",\"fromFieldOptional\":true}",
        RAC_INTEGRATION_ID = "{\"fromEntity\":\"Activity\",\"fromFieldName\":\"concatId\",\"fromFieldType\":\"String\",\"fromFieldOptional\":false}",
        SOURCE = "{\"fromEntity\":\"Activity\",\"fromFieldName\":\"sourceName\",\"fromFieldType\":\"String\",\"fromFieldOptional\":false}",
        COUNTRY_CODE = "{\"fromEntity\":\"Activity\",\"fromFieldName\":\"countryCode\",\"fromFieldType\":\"String\",\"fromFieldOptional\":false}",
        CREATED_AT = "{\"fromEntity\":\"Activity\",\"fromFieldName\":\"ohubCreated\",\"fromFieldType\":\"Timestamp\",\"fromFieldOptional\":false,\"datePattern\":\"yyyy-MM-dd HH:mm:ss\",\"exampleDate\":\"" + exampleDateTime + "\"}",
        UPDATED_AT = "{\"fromEntity\":\"Activity\",\"fromFieldName\":\"ohubUpdated\",\"fromFieldType\":\"Timestamp\",\"fromFieldOptional\":false,\"datePattern\":\"yyyy-MM-dd HH:mm:ss\",\"exampleDate\":\"" + exampleDateTime + "\"}",
        DELETE_FLAG = "{\"fromEntity\":\"Activity\",\"fromFieldName\":\"isActive\",\"fromFieldType\":\"boolean\",\"fromFieldOptional\":true,\"transform\":{\"function\":\"InvertedBooleanToYNConverter$\",\"description\":\"Inverts the value and converts it to Y(es) or N(o). f.e. true wil become \\\"N\\\"\"}}",
        CONTACT_DATE = "{\"fromEntity\":\"Activity\",\"fromFieldName\":\"activityDate\",\"fromFieldType\":\"Timestamp\",\"fromFieldOptional\":true,\"datePattern\":\"yyyy-MM-dd HH:mm:ss\",\"exampleDate\":\"" + exampleDateTime + "\"}",
        ACTION_TYPE = "{\"fromEntity\":\"Activity\",\"fromFieldName\":\"actionType\",\"fromFieldType\":\"String\",\"fromFieldOptional\":true}",
        ACTIVITY_NAME = "{\"fromEntity\":\"Activity\",\"fromFieldName\":\"name\",\"fromFieldType\":\"String\",\"fromFieldOptional\":true}",
        ACTIVITY_DETAILS = "{\"fromEntity\":\"Activity\",\"fromFieldName\":\"details\",\"fromFieldType\":\"String\",\"fromFieldOptional\":true}"
      )

      result shouldBe expectedActivity
    }
  }
}
