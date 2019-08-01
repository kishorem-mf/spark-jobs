package com.unilever.ohub.spark.domain.entity

object Util {

  def createConcatIdFromValues(countryCode: String, sourceName: String, sourceEntityId: String): String =
    s"$countryCode~$sourceName~$sourceEntityId"
}
