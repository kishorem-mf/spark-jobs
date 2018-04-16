package com.unilever.ohub.spark.tsv2parquet

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.sql._

import scala.reflect.runtime.universe._

object DomainGateKeeper {
  type ErrorMessage = String

  object implicits {
    // if we upgrade our scala version, we can probably get rid of this encoder too (because Either has become a Product in scala 2.12)
    implicit def eitherEncoder[T1, T2]: Encoder[Either[T1, T2]] =
      Encoders.kryo[Either[T1, T2]]
  }
}

abstract class DomainGateKeeper[DomainType <: DomainEntity: TypeTag, RowType] extends SparkJob {
  import DomainGateKeeper._
  import DomainGateKeeper.implicits._

  override final val neededFilePaths = Array("INPUT", "OUTPUT_FILE")

  protected[tsv2parquet] def partitionByValue: Seq[String]

  protected[tsv2parquet] def toDomainEntity: DomainTransformer ⇒ RowType ⇒ DomainType

  protected[tsv2parquet] def postValidate: DomainDataProvider ⇒ DomainEntity ⇒ Unit = dataProvider ⇒ DomainEntity.postConditions(dataProvider)

  protected def read(spark: SparkSession, storage: Storage, input: String): Dataset[RowType]

  private def transform(transformFn: RowType ⇒ DomainType)(postValidateFn: DomainEntity ⇒ Unit): RowType ⇒ Either[ErrorMessage, DomainType] =
    row ⇒
      try {
        val entity = transformFn(row)
        postValidateFn(entity)
        Right(entity)
      } catch {
        case e: Throwable ⇒
          Left(s"Error parsing row: '$e', row = '$row'")
      }

  override def run(spark: SparkSession, filePaths: Product, storage: Storage): Unit = {
    run(spark, filePaths, storage, DomainDataProvider(spark, storage))
  }

  protected[tsv2parquet] def run(spark: SparkSession, filePaths: Product, storage: Storage, dataProvider: DomainDataProvider): Unit = {
    import spark.implicits._

    val (input: String, outputFile: String) = filePaths
    val transformer = DomainTransformer(dataProvider)

    val result = read(spark, storage, input)
      .map(transform(toDomainEntity(transformer))(postValidate(dataProvider)))
      .distinct()
      // persist the result here (result is evaluated multiple times, since spark transformations are lazy)
      .persist(StorageLevels.MEMORY_AND_DISK)

    val errors: Dataset[ErrorMessage] = result.filter(_.isLeft).map(_.left.get)
    val numberOfErrors = errors.count()

    if (numberOfErrors > 0) { // do something with the errors here
      log.error(s"No parquet file written, number of errors found is '$numberOfErrors'")
      errors.toDF("ERROR").show(numRows = 100, truncate = false)
      System.exit(1) // let's fail fast now
    }

    val domainEntities: Dataset[DomainType] = result.filter(_.isRight).map(_.right.get)
    storage.writeToParquet(domainEntities, outputFile, partitionByValue)
  }
}
