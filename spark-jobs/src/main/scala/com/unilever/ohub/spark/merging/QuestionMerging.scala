package com.unilever.ohub.spark.merging

import java.util.UUID

import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import com.unilever.ohub.spark.domain.entity.Question
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

case class QuestionMergingConfig(
    questions: String = "question-input-file",
    previousIntegrated: String = "previous-integrated-file",
    outputFile: String = "path-to-output-file"
) extends SparkJobConfig

object QuestionMerging extends SparkJob[QuestionMergingConfig] {

  def transform(
    spark: SparkSession,
    questions: Dataset[Question],
    previousIntegrated: Dataset[Question]
  ): Dataset[Question] = {
    import spark.implicits._

    previousIntegrated
      .joinWith(questions, previousIntegrated("concatId") === questions("concatId"), JoinType.FullOuter)
      .map {
        case (integrated, question) ⇒
          if (question == null) {
            integrated
          } else {
            val ohubId = if (integrated == null) Some(UUID.randomUUID().toString) else integrated.ohubId

            question.copy(ohubId = ohubId, isGoldenRecord = true)
          }
      }
  }

  override private[spark] def defaultConfig = QuestionMergingConfig()

  override private[spark] def configParser(): OptionParser[QuestionMergingConfig] =
    new scopt.OptionParser[QuestionMergingConfig]("Question merging") {
      head("merges questions into an integrated questions output file.", "1.0")
      opt[String]("questionsInputFile") required () action { (x, c) ⇒
        c.copy(questions = x)
      } text "questionsInputFile is a string property"
      opt[String]("previousIntegrated") required () action { (x, c) ⇒
        c.copy(previousIntegrated = x)
      } text "previousIntegrated is a string property"
      opt[String]("outputFile") required () action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"

      version("1.0")
      help("help") text "help text"
    }

  override def run(spark: SparkSession, config: QuestionMergingConfig, storage: Storage): Unit = {
    log.info(
      s"Merging questions from [${config.questions}] and [${config.previousIntegrated}] to [${config.outputFile}]"
    )

    val questions = storage.readFromParquet[Question](config.questions)
    val previousIntegrated = storage.readFromParquet[Question](config.previousIntegrated)
    val transformed = transform(spark, questions, previousIntegrated)

    storage.writeToParquet(transformed, config.outputFile)
  }
}
