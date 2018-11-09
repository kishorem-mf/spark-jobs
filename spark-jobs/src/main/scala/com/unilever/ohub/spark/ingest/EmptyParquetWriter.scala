package com.unilever.ohub.spark.ingest

import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.domain.entity._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.Dataset

trait EmptyParquetWriter[T <: DomainEntity] {
  def createEmptyDataset(spark: SparkSession): Dataset[T]

  def writeEmptyParquet(spark: SparkSession, storage: Storage, location: String): Unit = {
    val ds = createEmptyDataset(spark)

    storage.writeToParquet(ds, location, saveMode = SaveMode.Ignore) // prevent overwriting existing data
  }
}

trait ContactPersonEmptyParquetWriter extends EmptyParquetWriter[ContactPerson] {

  def createEmptyDataset(spark: SparkSession): Dataset[ContactPerson] = {
    import spark.implicits._

    spark.createDataset[ContactPerson](Seq[ContactPerson]())
  }
}

trait OperatorEmptyParquetWriter extends EmptyParquetWriter[Operator] {

  def createEmptyDataset(spark: SparkSession): Dataset[Operator] = {
    import spark.implicits._

    spark.createDataset[Operator](Seq[Operator]())
  }
}

trait ProductEmptyParquetWriter extends EmptyParquetWriter[Product] {

  def createEmptyDataset(spark: SparkSession): Dataset[Product] = {
    import spark.implicits._

    spark.createDataset[Product](Seq[Product]())
  }
}

trait OrderEmptyParquetWriter extends EmptyParquetWriter[Order] {

  def createEmptyDataset(spark: SparkSession): Dataset[Order] = {
    import spark.implicits._

    spark.createDataset[Order](Seq[Order]())
  }
}

trait OrderLineEmptyParquetWriter extends EmptyParquetWriter[OrderLine] {

  def createEmptyDataset(spark: SparkSession): Dataset[OrderLine] = {
    import spark.implicits._

    spark.createDataset[OrderLine](Seq[OrderLine]())
  }
}

trait SubscriptionEmptyParquetWriter extends EmptyParquetWriter[Subscription] {

  def createEmptyDataset(spark: SparkSession): Dataset[Subscription] = {
    import spark.implicits._

    spark.createDataset[Subscription](Seq[Subscription]())
  }
}

trait QuestionEmptyParquetWriter extends EmptyParquetWriter[Question] {

  def createEmptyDataset(spark: SparkSession): Dataset[Question] = {
    import spark.implicits._

    spark.createDataset[Question](Seq[Question]())
  }
}

trait ActivityEmptyParquetWriter extends EmptyParquetWriter[Activity] {

  def createEmptyDataset(spark: SparkSession): Dataset[Activity] = {
    import spark.implicits._

    spark.createDataset[Activity](Seq[Activity]())
  }
}

trait AnswerEmptyParquetWriter extends EmptyParquetWriter[Answer] {

  def createEmptyDataset(spark: SparkSession): Dataset[Answer] = {
    import spark.implicits._

    spark.createDataset[Answer](Seq[Answer]())
  }
}