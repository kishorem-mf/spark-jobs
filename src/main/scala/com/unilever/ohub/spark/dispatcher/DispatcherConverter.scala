package com.unilever.ohub.spark.dispatcher

trait DispatcherConverter {

  private val outputCsvDelimiter: String = "\u00B6"

  val extraWriteOptions = Map(
    "delimiter" -> outputCsvDelimiter
  )
}
