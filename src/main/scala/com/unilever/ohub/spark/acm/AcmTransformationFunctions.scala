package com.unilever.ohub.spark.acm

import java.sql.Timestamp
import java.time.format.DateTimeFormatter

import com.unilever.ohub.spark.generic.StringFunctions

trait AcmTransformationFunctions {

  val dateFormat = "yyyy-MM-dd HH:mm:ss"

  val boolAsString = (bool: Boolean) ⇒ if (bool) "Y" else "N"

  val clean = (str: String) ⇒ StringFunctions.removeGenericStrangeChars(str)

  val cleanNames = (firstName: String, lastName: String, isFirstName: Boolean) ⇒ {
    StringFunctions.fillLastNameOnlyWhenFirstEqualsLastName(firstName, lastName, isFirstName)
  }

  def formatWithPattern(dateTimePattern: String = dateFormat)(input: Timestamp): String = {
    val pattern = DateTimeFormatter.ofPattern(dateTimePattern)
    pattern.format(input.toLocalDateTime)
  }
}
