package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import com.unilever.ohub.spark.domain.entity.{ Answer, ContactPerson }
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

case class AnswerMergingConfig(
    contactPersonInputFile: String = "contact-person-input-file",
    answers: String = "answer-input-file",
    previousIntegrated: String = "previous-integrated-file",
    outputFile: String = "path-to-output-file"
) extends SparkJobConfig

object AnswerMerging extends SparkJob[AnswerMergingConfig] {

  def transform(
    spark: SparkSession,
    answers: Dataset[Answer],
    previousIntegrated: Dataset[Answer]
  ): Dataset[Answer] = {
    import spark.implicits._

    previousIntegrated
      .joinWith(answers, previousIntegrated("concatId") === answers("concatId"), JoinType.FullOuter)
      .map {
        case (integrated, answer) ⇒
          if (answer == null) {
            integrated
          } else if (integrated == null) {
            answer
          } else {
            answer.copy(ohubId = integrated.ohubId, ohubCreated = integrated.ohubCreated)
          }
      }
  }

  override private[spark] def defaultConfig = AnswerMergingConfig()

  override private[spark] def configParser(): OptionParser[AnswerMergingConfig] =
    new scopt.OptionParser[AnswerMergingConfig]("Answer merging") {
      head("merges answers into an integrated answers output file.", "1.0")
      opt[String]("answersInputFile") required () action { (x, c) ⇒
        c.copy(answers = x)
      } text "answersInputFile is a string property"
      opt[String]("previousIntegrated") required () action { (x, c) ⇒
        c.copy(previousIntegrated = x)
      } text "previousIntegrated is a string property"
      opt[String]("outputFile") required () action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"

      version("1.0")
      help("help") text "help text"
    }

  override def run(spark: SparkSession, config: AnswerMergingConfig, storage: Storage): Unit = {
    import spark.implicits._

    log.info(
      s"Merging answers from [${config.answers}], [${config.contactPersonInputFile}] and [${config.previousIntegrated}] to [${config.outputFile}]"
    )

    val answers = storage.readFromParquet[Answer](config.answers)
    val previousIntegrated = storage.readFromParquet[Answer](config.previousIntegrated)
    val transformed = transform(spark, answers, previousIntegrated)

    storage.writeToParquet(transformed, config.outputFile)
  }
}
