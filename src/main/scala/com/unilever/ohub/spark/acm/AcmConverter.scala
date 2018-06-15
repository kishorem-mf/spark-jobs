package com.unilever.ohub.spark.acm

trait AcmConverter {

  private val outputCsvDelimiter: String = "\u00B6"

  val writeOptions = Map(
    "delimiter" -> outputCsvDelimiter
  )
}
