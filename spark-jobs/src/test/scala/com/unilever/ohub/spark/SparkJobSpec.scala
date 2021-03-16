package com.unilever.ohub.spark

import com.unilever.ohub.spark.SharedSparkSession.spark
import org.apache.spark.sql.{Dataset, Encoder}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FunSpec, Matchers}


trait SparkJobSpec extends FunSpec with Matchers with MockFactory {

  protected implicit class ObjOps[T: Encoder](obj: T) {
    def toDataset: Dataset[T] = spark.createDataset(Seq(obj))
  }

  protected implicit class ObjsOps[T: Encoder](objs: Seq[T]) {
    def toDataset: Dataset[T] = spark.createDataset(objs)
  }
}
