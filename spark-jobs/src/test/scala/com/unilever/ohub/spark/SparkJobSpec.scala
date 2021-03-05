package com.unilever.ohub.spark

import org.apache.spark.sql.{Dataset, Encoder}
import org.scalatest.{FunSpec, Matchers}
import SharedSparkSession.spark
import com.unilever.ohub.spark.storage.Storage
import org.scalamock.scalatest.MockFactory

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SaveMode }


trait SparkJobSpec extends FunSpec with Matchers with MockFactory {

  protected implicit class ObjOps[T: Encoder](obj: T) {
    def toDataset: Dataset[T] = spark.createDataset(Seq(obj))
  }

  protected implicit class ObjsOps[T: Encoder](objs: Seq[T]) {
    def toDataset: Dataset[T] = spark.createDataset(objs)
  }
}
