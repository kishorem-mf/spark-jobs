package com.unilever.ohub.spark.tsv2parquet

import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql._
import scopt.OptionParser
import DomainGateKeeper._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import scala.reflect.runtime.universe._

object DomainGateKeeper {
  case class ErrorMessage(error: String, row: String)

  case class DomainConfig(
      inputFile: String = "path-to-input-file",
      outputFile: String = "path-to-output-file",
      fieldSeparator: String = "field-separator",
      strictIngestion: Boolean = true,
      showErrorSummary: Boolean = true,
      postgressUrl: String = "postgress-url",
      postgressUsername: String = "postgress-username",
      postgressPassword: String = "postgress-password",
      postgressDB: String = "postgress-db"
  ) extends SparkJobConfig

  object implicits {
    // if we upgrade our scala version, we can probably get rid of this encoder too (because Either has become a Product in scala 2.12)
    implicit def eitherEncoder[T1, T2]: Encoder[Either[T1, T2]] =
      Encoders.kryo[Either[T1, T2]]
  }
}

abstract class DomainGateKeeper[DomainType <: DomainEntity: TypeTag, RowType] extends SparkJob[DomainConfig] {

  import DomainGateKeeper.implicits._

  protected[tsv2parquet] def partitionByValue: Seq[String]

  protected[tsv2parquet] def toDomainEntity: DomainTransformer ⇒ RowType ⇒ DomainType

  protected[tsv2parquet] def postValidate: DomainDataProvider ⇒ DomainEntity ⇒ Unit = dataProvider ⇒ DomainEntity.postConditions(dataProvider)

  private[spark] def defaultConfig: DomainConfig = DomainConfig()

  private[spark] def configParser(): OptionParser[DomainConfig] =
    new scopt.OptionParser[DomainConfig]("Domain gate keeper") {
      head("converts a csv into domain entities and writes the result to parquet.", "1.0")
      opt[String]("inputFile") required () action { (x, c) ⇒
        c.copy(inputFile = x)
      } text "inputFile is a string property"
      opt[String]("outputFile") required () action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"
      opt[String]("fieldSeparator") optional () action { (x, c) ⇒
        c.copy(fieldSeparator = x)
      } text "fieldSeparator is a string property"
      opt[Boolean]("strictIngestion") optional () action { (x, c) ⇒
        c.copy(strictIngestion = x)
      } text "strictIngestion is a boolean property"
      opt[Boolean]("showErrorSummary") optional () action { (x, c) ⇒
        c.copy(showErrorSummary = x)
      } text "showErrorSummary is a boolean property"
      opt[String]("postgressUrl") required () action { (x, c) ⇒
        c.copy(postgressUrl = x)
      } text "postgressUrl is a string property"
      opt[String]("postgressUsername") required () action { (x, c) ⇒
        c.copy(postgressUsername = x)
      } text "postgressUsername is a string property"
      opt[String]("postgressPassword") required () action { (x, c) ⇒
        c.copy(postgressPassword = x)
      } text "postgressPassword is a string property"
      opt[String]("postgressDB") required () action { (x, c) ⇒
        c.copy(postgressDB = x)
      } text "postgressDB is a string property"

      version("1.0")
      help("help") text "help text"
    }

  protected def read(spark: SparkSession, storage: Storage, config: DomainConfig): Dataset[RowType]

  private def transform(transformFn: RowType ⇒ DomainType)(postValidateFn: DomainEntity ⇒ Unit): RowType ⇒ Either[ErrorMessage, DomainType] =
    row ⇒
      try {
        val entity = transformFn(row)
        postValidateFn(entity)
        Right(entity)
      } catch {
        case e: Throwable ⇒
          Left(ErrorMessage(s"Error parsing row: '$e'", s"$row"))
      }

  override def run(spark: SparkSession, config: DomainConfig, storage: Storage): Unit = {
    val dataProvider = new PostgressDomainDataProvider(spark, config.postgressUrl, config.postgressDB, config.postgressUsername, config.postgressPassword)
    run(spark, config, storage, dataProvider)
  }

  protected[tsv2parquet] def run(spark: SparkSession, config: DomainConfig, storage: Storage, dataProvider: DomainDataProvider): Unit = {
    import spark.implicits._

    def handleErrors(config: DomainConfig, result: Dataset[Either[ErrorMessage, DomainType]]): Unit = {
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

    val transformer = DomainTransformer(dataProvider)

    val result = read(spark, storage, config)
      .map(transform(toDomainEntity(transformer))(postValidate(dataProvider)))

    handleErrors(config, result)

    val w = Window.partitionBy($"concatId").orderBy($"dateUpdated".desc_nulls_last)
    val domainEntities: Dataset[DomainType] = result
      .filter(_.isRight).map(_.right.get)
      .withColumn("rn", row_number.over(w))
      .filter($"rn" === 1)
      .drop($"rn")
      .as[DomainType]

    storage.writeToParquet(domainEntities, config.outputFile, partitionByValue)
  }
}
