package com.unilever.ohub.spark.outbound

import com.unilever.ohub.spark.domain.{DomainEntity, DomainEntityHash}
import com.unilever.ohub.spark.storage.DefaultStorage
import org.apache.spark.sql.{Column, Dataset, Encoders, SparkSession}

class InMemStorage[DomainType <: DomainEntity](spark: SparkSession, entities: Dataset[DomainType], hashes: Dataset[DomainEntityHash]) extends DefaultStorage(spark) {

  import scala.reflect.runtime.universe._

  override def readFromParquet[T <: Product : TypeTag](location: String, selectColumns: Seq[Column] = Seq()): Dataset[T] = {
    implicit val encoder = Encoders.product[T]

    if (location == "integrated") {
      entities.as[T]
    } else {
      hashes.as[T]
    }
  }
}
