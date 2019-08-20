package com.unilever.ohub.spark.export.acm

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

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

    it("should be able to explain the origin of the fields") {
      val activity = defaultActivity.copy(dateCreated = Some(Timestamp.valueOf("2015-06-30 13:47:00.0")), dateUpdated = Some(Timestamp.valueOf("2015-06-30 13:47:00.0")))
      val result = SUT.convert(activity, true)

      val exampleDateTime = DateTimeFormatter.ofPattern(SUT.timestampPattern).format(LocalDateTime.now().truncatedTo(ChronoUnit.HOURS))

      val expectedAcmActivity = AcmActivity(
        ACTIVITY_ID = "{\"fromEntity\":\"Activity\",\"fromFieldName\":\"concatId\",\"fromFieldType\":\"String\",\"fromFieldOptional\":false}",
        COUNTRY_CODE = "{\"fromEntity\":\"Activity\",\"fromFieldName\":\"countryCode\",\"fromFieldType\":\"String\",\"fromFieldOptional\":false}",
        CP_ORIG_INTEGRATION_ID = "{\"fromEntity\":\"Activity\",\"fromFieldName\":\"contactPersonOhubId\",\"fromFieldType\":\"String\",\"fromFieldOptional\":true}",
        DELETE_FLAG = "{\"fromEntity\":\"Activity\",\"fromFieldName\":\"isActive\",\"fromFieldType\":\"boolean\",\"fromFieldOptional\":true,\"transform\":{\"function\":\"InvertedBooleanToYNConverter$\",\"description\":\"Inverts the value and converts it to Y(es) or N(o). f.e. true wil become \\\"N\\\"\"}}",
        DATE_CREATED = "{\"fromEntity\":\"Activity\",\"fromFieldName\":\"dateCreated\",\"fromFieldType\":\"Timestamp\",\"fromFieldOptional\":true,\"datePattern\":\"yyyy/MM/dd HH:mm:ss\",\"exampleDate\":\"" + exampleDateTime + "\"}",
        DATE_UPDATED = "{\"fromEntity\":\"Activity\",\"fromFieldName\":\"dateUpdated\",\"fromFieldType\":\"Timestamp\",\"fromFieldOptional\":true,\"datePattern\":\"yyyy/MM/dd HH:mm:ss\",\"exampleDate\":\"" + exampleDateTime + "\"}",
        DETAILS = "{\"fromEntity\":\"Activity\",\"fromFieldName\":\"details\",\"fromFieldType\":\"String\",\"fromFieldOptional\":true}",
        TYPE = "{\"fromEntity\":\"Activity\",\"fromFieldName\":\"actionType\",\"fromFieldType\":\"String\",\"fromFieldOptional\":true}",
        NAME = "{\"fromEntity\":\"Activity\",\"fromFieldName\":\"name\",\"fromFieldType\":\"String\",\"fromFieldOptional\":true}")

      result shouldBe expectedAcmActivity
    }
  }
}
