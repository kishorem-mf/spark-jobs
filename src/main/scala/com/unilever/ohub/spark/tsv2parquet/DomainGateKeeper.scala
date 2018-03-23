package com.unilever.ohub.spark.tsv2parquet

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql._

import scala.reflect.runtime.universe._

object DomainGateKeeper {
  type ErrorMessage = String

  object implicits {

    implicit val errorMessageEncoder: Encoder[String] = Encoders.STRING

    implicit def toTypeEncoder[T <: Product : TypeTag]: Encoder[T] =
      Encoders.product[T]

    implicit def eitherEncoder[T1, T2]: Encoder[Either[T1, T2]] =
      Encoders.kryo[Either[T1, T2]]
  }
}

abstract class DomainGateKeeper[ToType <: Product : TypeTag] extends SparkJob {
  import DomainGateKeeper._
  import DomainGateKeeper.implicits._

  protected def fieldSeparator: String
  protected def hasHeaders: Boolean
  protected def partitionByValue: Seq[String]

  protected def transform: Row => Either[ErrorMessage, ToType]

  override final val neededFilePaths = Array("INPUT_FILE", "OUTPUT_FILE")

  override def run(spark: SparkSession, filePaths: Product, storage: Storage): Unit = {

    val (inputFile: String, outputFile: String) = filePaths

    val result = storage
      .readFromCsv(
        location = inputFile,
        fieldSeparator = fieldSeparator,
        hasHeaders = hasHeaders
      )
      .map(transform)
//      .persist(StorageLevel.MEMORY_AND_DISK) // we could persist the result to improve performance (now the result DS will be evaluated twice)

    val errors = result.filter(_.isLeft).map(_.left.get)
    // do something with the errors here
    errors.show(numRows = 100, truncate = false)

    // only the correct results are written to parquet
    val validEntities: Dataset[ToType] = result.filter(_.isRight).map(_.right.get)
    storage.writeToParquet(validEntities, outputFile, partitionByValue)
  }
}
