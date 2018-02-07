package com.unilever.ohub.spark

import org.apache.spark.sql.{ Dataset, Encoder }
import org.scalatest.{ FunSpec, Matchers }
import SharedSparkSession.spark

trait SparkJobSpec extends FunSpec with Matchers {
  protected implicit class ObjOps[T: Encoder](obj: T) {
    def toDataset: Dataset[T] = spark.createDataset(Seq(obj))
  }
}
