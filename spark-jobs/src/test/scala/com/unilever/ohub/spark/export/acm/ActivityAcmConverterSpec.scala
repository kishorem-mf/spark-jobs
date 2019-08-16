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

    it("should be able to explain the origin of the fields") {
      val activity = defaultActivity.copy(dateCreated = Some(Timestamp.valueOf("2015-06-30 13:47:00.0")), dateUpdated = Some(Timestamp.valueOf("2015-06-30 13:47:00.0")))
      val result = SUT.convert(activity, true)

      val expectedAcmActivity = AcmActivity(
        ACTIVITY_ID = "{\"fromEntity\":\"Activity\",\"fromFieldName\":\"concatId\",\"fromFieldType\":\"java.lang.String\"}",
        COUNTRY_CODE = "{\"fromEntity\":\"Activity\",\"fromFieldName\":\"countryCode\",\"fromFieldType\":\"java.lang.String\"}",
        CP_ORIG_INTEGRATION_ID = "{\"fromEntity\":\"Activity\",\"fromFieldName\":\"contactPersonOhubId\",\"fromFieldType\":\"scala.Option<java.lang.String>\"}",
        DELETE_FLAG = "{\"fromEntity\":\"Activity\",\"fromFieldName\":\"isActive\",\"fromFieldType\":\"boolean\",\"transform\":{\"function\":\"InvertedBooleanToYNConverter$\",\"description\":\"Inverts the value and converts it to Y(es) or N(o). f.e. true wil become \\\"N\\\"\"}}",
        DATE_CREATED = "{\"fromEntity\":\"Activity\",\"fromFieldName\":\"dateCreated\",\"fromFieldType\":\"scala.Option<java.sql.Timestamp>\"}",
        DATE_UPDATED = "{\"fromEntity\":\"Activity\",\"fromFieldName\":\"dateUpdated\",\"fromFieldType\":\"scala.Option<java.sql.Timestamp>\"}",
        DETAILS = "{\"fromEntity\":\"Activity\",\"fromFieldName\":\"details\",\"fromFieldType\":\"scala.Option<java.lang.String>\"}",
        TYPE = "{\"fromEntity\":\"Activity\",\"fromFieldName\":\"actionType\",\"fromFieldType\":\"scala.Option<java.lang.String>\"}",
        NAME = "{\"fromEntity\":\"Activity\",\"fromFieldName\":\"name\",\"fromFieldType\":\"scala.Option<java.lang.String>\"}")

      result shouldBe expectedAcmActivity
    }
  }
}
