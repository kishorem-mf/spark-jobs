package com.unilever.ohub.spark.data

  case class EmQuestionRecord(
    questionId: Option[Long],
    countryCode: Option[String], // enum
    competitionEntryFk: Option[Long],
    question: Option[String])
