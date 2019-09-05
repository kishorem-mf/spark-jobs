package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.export.TransformationFunction

object ToUpperCase extends TransformationFunction[String] {
  def impl(input: String):String = input.toUpperCase

  val description: String = "Converts the string to upperCase"
}
