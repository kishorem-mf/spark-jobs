package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

object TestAnswers extends TestAnswers

trait TestAnswers {

  lazy val defaultAnswer: Answer = Answer(
    concatId = "DE~EMAKINA~b3a6208c-d7f6-44e2-80e2-f26d461f64c0",
    countryCode = "DE",
    customerType = "CONTACTPERSON",
    isActive = true,
    sourceEntityId = "b3a6208c-d7f6-44e2-80e2-f26d461f64c0",
    sourceName = "EMAKINA",
    ohubCreated = Timestamp.valueOf("2015-06-30 13:49:00.0"),
    ohubUpdated = Timestamp.valueOf("2015-06-30 13:49:00.0"),
    dateCreated = None,
    dateUpdated = None,
    ohubId = null,
    isGoldenRecord = false,

    answer = Some("some answer"),
    questionConcatId = "DE~EMAKINA~456",

    additionalFields = Map(),
    ingestionErrors = Map()
  )
}
