package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.export.TypeConversionFunctions

trait DispatchTransformationFunctions extends TypeConversionFunctions {
  override val timestampPattern: String = "yyyy-MM-dd HH:mm:ss"
  override val datePattern: String = "yyyy-MM-dd"
}
