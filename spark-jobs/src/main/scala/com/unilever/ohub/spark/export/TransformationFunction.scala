package com.unilever.ohub.spark.export

trait TransformationFunction[T] {
  def impl(input: T): String
  val description: String
}

object InvertedBooleanToYNConverter extends TransformationFunction[Boolean] {
  def impl(bool: Boolean) = if (bool) "N" else "Y"
  override val description: String = "Function inverts the value and converts it to Y of N"
}
