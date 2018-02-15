package com.unilever.ohub.spark

import org.apache.spark.sql.{ Dataset, Encoder }
import org.scalatest.{ FunSpec, Matchers }
import SharedSparkSession.spark
import org.scalamock.scalatest.MockFactory

trait SparkJobSpec extends FunSpec with Matchers with MockFactory {
  protected implicit class ObjOps[T: Encoder](obj: T) {
    def toDataset: Dataset[T] = spark.createDataset(Seq(obj))
  }
}
