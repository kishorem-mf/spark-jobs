package com.unilever.ohub.spark.export.acm

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.unilever.ohub.spark.domain.entity.TestActivities
import com.unilever.ohub.spark.export.acm.model.AcmActivity
import com.unilever.ohub.spark.export.{FieldMapping, InvertedBooleanToYNConverter}
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

      val mapper = new ObjectMapper() with ScalaObjectMapper
      mapper.registerModule(DefaultScalaModule)

      val fieldMapping = FieldMapping(
        fromEntity = "Activity",
        fromField = "",
        fromType = "String",
        required = true
      )

      val expectedAcmActivity = AcmActivity(
        ACTIVITY_ID = mapper.writeValueAsString(fieldMapping.copy(fromField = "concatId")),
        COUNTRY_CODE = mapper.writeValueAsString(fieldMapping.copy(fromField = "countryCode")),
        CP_ORIG_INTEGRATION_ID = mapper.writeValueAsString(fieldMapping.copy(fromField = "contactPersonOhubId", required = false)),
        DELETE_FLAG = mapper.writeValueAsString(fieldMapping.copy(fromField = "isActive", fromType = "boolean", transformation = InvertedBooleanToYNConverter.description, exampleValue = InvertedBooleanToYNConverter.exampleValue)),
        DATE_CREATED = mapper.writeValueAsString(fieldMapping.copy(fromField = "dateCreated", fromType = "Timestamp", pattern = SUT.timestampPattern, exampleValue = exampleDateTime, required = false)),
        DATE_UPDATED = mapper.writeValueAsString(fieldMapping.copy(fromField = "dateUpdated", fromType = "Timestamp", pattern = SUT.timestampPattern, exampleValue = exampleDateTime, required = false)),
        DETAILS = mapper.writeValueAsString(fieldMapping.copy(fromField = "details", required = false)),
        TYPE = mapper.writeValueAsString(fieldMapping.copy(fromField = "actionType", required = false)),
        NAME = mapper.writeValueAsString(fieldMapping.copy(fromField = "name", required = false)))

      result shouldBe expectedAcmActivity
    }
  }
}
