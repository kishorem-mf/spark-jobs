package com.unilever.ohub.spark

import org.apache.spark.sql.{ Dataset, Encoder }
import org.scalatest.{ FunSpec, Matchers }
import SharedSparkSession.spark
import org.scalamock.scalatest.MockFactory

trait SparkJobSpec extends SparkJobSpec {

  protected implicit class ObjOps[T: Encoder](obj: T) {
    def toDataset: Dataset[T] = spark.createDataset(Seq(obj))
  }

  protected implicit class ObjsOps[T: Encoder](objs: Seq[T]) {
    def toDataset: Dataset[T] = spark.createDataset(objs)
  }
}
