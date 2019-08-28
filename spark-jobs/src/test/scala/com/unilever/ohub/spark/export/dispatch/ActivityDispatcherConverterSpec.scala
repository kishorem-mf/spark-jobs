package com.unilever.ohub.spark.export.dispatch

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.TestActivities
import com.unilever.ohub.spark.export.dispatch.model.DispatchActivity
import com.unilever.ohub.spark.export.{FieldMapping, InvertedBooleanToYNConverter}

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

      val mapper = new ObjectMapper() with ScalaObjectMapper
      mapper.registerModule(DefaultScalaModule)

      val fieldMapping = FieldMapping(
        fromEntity = "Activity",
        fromField = "",
        fromType = "String",
        required = true
      )

      val expectedActivity = DispatchActivity(
        CP_ORIG_INTEGRATION_ID = mapper.writeValueAsString(fieldMapping.copy(fromField = "contactPersonConcatId", required = false)),
        RAC_INTEGRATION_ID = mapper.writeValueAsString(fieldMapping.copy(fromField = "concatId")),
        SOURCE = mapper.writeValueAsString(fieldMapping.copy(fromField = "sourceName")),
        COUNTRY_CODE = mapper.writeValueAsString(fieldMapping.copy(fromField = "countryCode")),
        CREATED_AT = mapper.writeValueAsString(fieldMapping.copy(fromField = "ohubCreated", fromType = "Timestamp", pattern = SUT.timestampPattern, exampleValue = exampleDateTime)),
        UPDATED_AT = mapper.writeValueAsString(fieldMapping.copy(fromField = "ohubUpdated", fromType = "Timestamp", pattern = SUT.timestampPattern, exampleValue = exampleDateTime)),
        DELETE_FLAG = mapper.writeValueAsString(fieldMapping.copy(fromField = "isActive", fromType = "boolean", transformation = InvertedBooleanToYNConverter.description, exampleValue = InvertedBooleanToYNConverter.exampleValue)),
        CONTACT_DATE = mapper.writeValueAsString(fieldMapping.copy(fromField = "activityDate", fromType = "Timestamp", pattern = SUT.timestampPattern, exampleValue = exampleDateTime, required = false)),
        ACTION_TYPE = mapper.writeValueAsString(fieldMapping.copy(fromField = "actionType", required = false)),
        ACTIVITY_NAME = mapper.writeValueAsString(fieldMapping.copy(fromField = "name", required = false)),
        ACTIVITY_DETAILS = mapper.writeValueAsString(fieldMapping.copy(fromField = "details", required = false))
      )

      result shouldBe expectedActivity
    }
  }
}
