package com.unilever.ohub.spark.tsv2parquet

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql._

import scala.reflect.runtime.universe._

object DomainGateKeeper {
  type ErrorMessage = String

  object implicits {

    implicit val errorMessageEncoder: Encoder[String] = Encoders.STRING

    implicit def domainEntityEncoder[T <: DomainEntity : TypeTag]: Encoder[T] =
      Encoders.product[T]

    implicit def eitherEncoder[T1, T2]: Encoder[Either[T1, T2]] =
      Encoders.kryo[Either[T1, T2]]
  }
}

abstract class DomainGateKeeper[DomainType <: DomainEntity : TypeTag] extends SparkJob {
  import DomainGateKeeper._
  import DomainGateKeeper.implicits._

  protected def fieldSeparator: String

  protected def hasHeaders: Boolean

  protected def partitionByValue: Seq[String]

  override final val neededFilePaths = Array("INPUT_FILE", "OUTPUT_FILE")

  def transform(toDomainEntity: Row => DomainType): Row => Either[ErrorMessage, DomainType] =
    row =>
      try
        Right(toDomainEntity(row))
      catch {
        case e: Exception =>
          Left(s"Error parsing row: '$e', row = '$row'")
      }

  def toDomainEntity: Row => DomainType

  override def run(spark: SparkSession, filePaths: Product, storage: Storage): Unit = {
    val (inputFile: String, outputFile: String) = filePaths

    val result = storage
      .readFromCsv(
        location = inputFile,
        fieldSeparator = fieldSeparator,
        hasHeaders = hasHeaders
      )
      .map(transform(toDomainEntity))

    val errors: Dataset[ErrorMessage] = result.filter(_.isLeft).map(_.left.get)
    val numberOfErrors = errors.count()

    if (numberOfErrors > 0) { // do something with the errors here
      log.error(s"No parquet file written, number of errors found is '$numberOfErrors'")
      errors.toDF("ERROR").show(numRows = 100, truncate = false)
    } else {
      val domainEntities: Dataset[DomainType] = result.filter(_.isRight ).map(_.right.get)
      storage.writeToParquet(domainEntities, outputFile, partitionByValue)
    }
  }
}
