package com.unilever.ohub.spark.data

  case class EmAnswerRecord(
    answerId: Option[Long],
    countryCode: Option[String],
    questionFk: Option[String],
    answer: Option[String],
    selected: Option[Boolean], // 1/0
    openAnswer: Option[String])
