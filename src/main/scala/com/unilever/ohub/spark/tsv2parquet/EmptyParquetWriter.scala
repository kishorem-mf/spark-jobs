package com.unilever.ohub.spark.tsv2parquet

import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.SparkSession

trait EmptyParquetWriter {
  def writeEmptyParquet(spark: SparkSession, storage: Storage, location: String): Unit
}

trait ContactPersonEmptyParquetWriter extends EmptyParquetWriter {
  override def writeEmptyParquet(spark: SparkSession, storage: Storage, location: String): Unit = {
    import com.unilever.ohub.spark.domain.entity.ContactPerson
    import spark.implicits._

    val contactPersons = spark.createDataset[ContactPerson](Seq[ContactPerson]())

    storage.writeToParquet(contactPersons, location)
  }
}

trait OperatorEmptyParquetWriter extends EmptyParquetWriter {
  override def writeEmptyParquet(spark: SparkSession, storage: Storage, location: String): Unit = {
    import com.unilever.ohub.spark.domain.entity.Operator
    import spark.implicits._

    val operators = spark.createDataset[Operator](Seq[Operator]())

    storage.writeToParquet(operators, location)
  }
}

trait ProductEmptyParquetWriter extends EmptyParquetWriter {
  override def writeEmptyParquet(spark: SparkSession, storage: Storage, location: String): Unit = {
    import com.unilever.ohub.spark.domain.entity.Product
    import spark.implicits._

    val ds = spark.createDataset[Product](Seq[Product]())

    storage.writeToParquet(ds, location)
  }
}

trait OrderEmptyParquetWriter extends EmptyParquetWriter {
  override def writeEmptyParquet(spark: SparkSession, storage: Storage, location: String): Unit = {
    import com.unilever.ohub.spark.domain.entity.Order
    import spark.implicits._

    val ds = spark.createDataset[Order](Seq[Order]())

    storage.writeToParquet(ds, location)
  }
}

trait OrderLineEmptyParquetWriter extends EmptyParquetWriter {
  override def writeEmptyParquet(spark: SparkSession, storage: Storage, location: String): Unit = {
    import com.unilever.ohub.spark.domain.entity.OrderLine
    import spark.implicits._

    val ds = spark.createDataset[OrderLine](Seq[OrderLine]())

    storage.writeToParquet(ds, location)
  }
}
