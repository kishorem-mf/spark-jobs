package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

object TestQuestions extends TestQuestions

trait TestQuestions {

  lazy val defaultQuestion: Question = Question(
    concatId = "DE~EMAKINA~b3a6208c-d7f6-44e2-80e2-f26d461f64c0",
    countryCode = "DE",
    customerType = "CONTACTPERSON",
    dateCreated = None,
    dateUpdated = None,
    isActive = true,
    isGoldenRecord = false,
    sourceEntityId = "b3a6208c-d7f6-44e2-80e2-f26d461f64c0",
    sourceName = "EMAKINA",
    ohubId = null,
    ohubCreated = Timestamp.valueOf("2015-06-30 13:49:00.0"),
    ohubUpdated = Timestamp.valueOf("2015-06-30 13:49:00.0"),

    question = Some("question"),
    activityConcatId = "DE~EMAKINA~456",

    additionalFields = Map(),
    ingestionErrors = Map()
  )
}
