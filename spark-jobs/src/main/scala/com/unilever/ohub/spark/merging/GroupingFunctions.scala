package com.unilever.ohub.spark.merging

import java.util.UUID

import org.apache.spark.sql.functions.udf

trait GroupingFunctions {

  val createOhubIdUdf = udf[String](() ⇒ UUID.randomUUID().toString)
}
