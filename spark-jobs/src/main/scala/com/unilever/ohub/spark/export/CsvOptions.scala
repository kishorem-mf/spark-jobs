package com.unilever.ohub.spark.export

trait CsvOptions {
  lazy final val options = Map("quote" -> "\"", "escape" -> "\"") ++ extraOptions ++ quotes
  val extraOptions = Map[String, String]()

  val mustQuotesFields = false
  val delimiter: String = ","
  val quotes = if (mustQuotesFields) Map("quoteAll" -> "true") else Map()
}
