package com.unilever.ohub.spark.export.acm

import com.unilever.ohub.spark.export.TypeConversionFunctions

trait AcmTypeConversionFunctions extends TypeConversionFunctions {
  override protected[export] val timestampPattern: String = "yyyy/MM/dd HH:mm:ss"
  override protected[export] val datePattern: String = "yyyy/MM/dd"
}
