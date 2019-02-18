package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.TestOperators
import org.apache.spark.sql.SparkSession

class OperatorIntegratedExactMatchSpec extends SparkJobSpec with TestOperators {

  lazy implicit val implicitSpark: SparkSession = spark

}
