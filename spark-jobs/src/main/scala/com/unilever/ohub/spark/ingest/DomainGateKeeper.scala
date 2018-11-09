package com.unilever.ohub.spark.ingest

import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql._
import DomainGateKeeper._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import scala.reflect.runtime.universe._

object DomainGateKeeper {
  case class ErrorMessage(error: String, row: String)

  trait DomainConfig extends SparkJobConfig {
    val outputFile: String = "path-to-output-file"
    val deduplicateOnConcatId: Boolean = true
    val strictIngestion: Boolean = true
    val showErrorSummary: Boolean = true
  }

  object implicits {
    // if we upgrade our scala version, we can probably get rid of this encoder too (because Either has become a Product in scala 2.12)
    implicit def eitherEncoder[T1, T2]: Encoder[Either[T1, T2]] =
      Encoders.kryo[Either[T1, T2]]
  }
}

abstract class DomainGateKeeper[DomainType <: DomainEntity: TypeTag, RowType, Config <: DomainConfig] extends SparkJob[Config] {

  import DomainGateKeeper.implicits._

  protected def partitionByValue: Seq[String]

  protected def toDomainEntity: DomainTransformer ⇒ RowType ⇒ DomainType

  private[spark] def writeEmptyParquet(spark: SparkSession, storage: Storage, location: String): Unit

  protected def read(spark: SparkSession, storage: Storage, config: Config): Dataset[RowType]

  private def transform(
    transformFn: DomainTransformer ⇒ RowType ⇒ DomainType
  ): RowType ⇒ Either[ErrorMessage, DomainType] =
    row ⇒
      try {
        val entity = transformFn(DomainTransformer())(row)
        Right(entity)
      } catch {
        case e: Throwable ⇒
          Left(ErrorMessage(s"Error parsing row: '$e'", s"$row"))
      }

  override def run(spark: SparkSession, config: Config, storage: Storage): Unit = {
    import spark.implicits._

    def handleErrors(config: Config, result: Dataset[Either[ErrorMessage, DomainType]]): Unit = {
      val errors: Dataset[ErrorMessage] = result.filter(_.isLeft).map(_.left.get)
      val numberOfErrors = errors.count()

      if (numberOfErrors > 0) { // do something with the errors here
        if (config.showErrorSummary) { // create a summary report
          errors
            .groupByKey(_.error)
            .count()
            .toDF("ERROR", "COUNT").show(numRows = 100, truncate = false)
        } else { // show plain errors
          errors
            .toDF("ERROR", "ROW").show(numRows = 100, truncate = false)
        }

        if (config.strictIngestion) {
          log.error(s"NO PARQUET FILE WRITTEN, NUMBER OF ERRORS FOUND IS '$numberOfErrors'")
          System.exit(1) // let's fail fast now
        } else {
          log.error(s"WRITE PARQUET FILE ANYWAY, REGARDLESS OF NUMBER OF ERRORS FOUND '$numberOfErrors' (ERRONEOUS ENTITIES ARE NEGLECTED). ")
        }
      }
    }

    val result = read(spark, storage, config)
      .map {
        transform(toDomainEntity)
      }

    handleErrors(config, result)

    // deduplicate incoming domain entities by selecting the 'newest' entity per unique concatId.
    val w = Window.partitionBy($"concatId").orderBy($"dateUpdated".desc_nulls_last)
    val domainEntitiesMapped: Dataset[DomainType] = result
      .filter(_.isRight).map(_.right.get)

    val domainEntities = if (config.deduplicateOnConcatId) {
      domainEntitiesMapped
        .withColumn("rn", row_number.over(w))
        .filter($"rn" === 1)
        .drop($"rn")
        .as[DomainType]
    } else {
      domainEntitiesMapped
    }

    if (domainEntities.head(1).isEmpty) {
      // note: add a check on #rows in raw, only if #rows is 0 an empty parquet file should be written
      log.warn(s"WRITING EMPTY PARQUET FILE FOR '${this.getClass.getName}'")
      writeEmptyParquet(spark, storage, config.outputFile)
    } else {
      storage.writeToParquet(domainEntities, config.outputFile, partitionByValue)
    }
  }
}