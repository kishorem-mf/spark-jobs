package com.unilever.ohub.spark.export.acm

import com.unilever.ohub.spark.export.{CleanString, TransformationFunction}


object InvertedBooleanTo10Converter extends TransformationFunction[Boolean] {
  def impl(bool: Boolean) = if (bool) "0" else "1"

  override val exampleValue: String = "1"
  val description: String = "Inverts the value and converts it to 1 or 0. f.e. true wil become \"0\""
}

object GenderToNumeric extends TransformationFunction[Option[String]] {
  def impl(gender: Option[String]) = {
    CleanString.impl(gender.getOrElse("")) match {
      case "M" ⇒ "1"
      case "F" ⇒ "2"
      case _ ⇒ "0"
    }
  }

  override val exampleValue: String = "1"
  val description = "Cleans the gender string and converts \"M\" -> \"1\", \"F\" -> \"2\" and otherwise \"0\""
}
