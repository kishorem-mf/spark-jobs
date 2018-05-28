package com.unilever.ohub.spark.combining

import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

case class ExactAndFuzzyMatchesConfig(
    updatedExactMatchInputFile: String = "updated-exact-match-input-file",
    ingestedExactMatchInputFile: String = "ingested-exact-match-input-file",
    fuzzyMatchCombinedInputFile: String = "fuzzy-match-combined-input-file",
    combinedOutputFile: String = "combined-output-file"
) extends SparkJobConfig

object ContactPersonCombineExactAndFuzzyMatches extends SparkJob[ExactAndFuzzyMatchesConfig] {

  override private[spark] def defaultConfig = ExactAndFuzzyMatchesConfig()

  override private[spark] def configParser(): OptionParser[ExactAndFuzzyMatchesConfig] =
    new scopt.OptionParser[ExactAndFuzzyMatchesConfig]("Contact person exact and fuzzy matches combiner") {
      head("combines entities from exact matches and fuzzy matches", "1.0")
      opt[String]("updatedExactMatchInputFile") required () action { (x, c) ⇒
        c.copy(updatedExactMatchInputFile = x)
      } text "updatedExactMatchInputFile is a string property"
      opt[String]("ingestedExactMatchInputFile") required () action { (x, c) ⇒
        c.copy(ingestedExactMatchInputFile = x)
      } text "ingestedExactMatchInputFile is a string property"
      opt[String]("fuzzyMatchCombinedInputFile") required () action { (x, c) ⇒
        c.copy(fuzzyMatchCombinedInputFile = x)
      } text "fuzzyMatchCombinedInputFile is a string property"
      opt[String]("combinedOutputFile") required () action { (x, c) ⇒
        c.copy(combinedOutputFile = x)
      } text "combinedOutputFile is a string property"

      version("1.0")
      help("help") text "help text"
    }

  def transform(
    spark: SparkSession,
    updatedExactMatchInput: Dataset[ContactPerson],
    ingestedExactMatchInput: Dataset[ContactPerson],
    fuzzyMatchCombinedInputFile: Dataset[ContactPerson]
  ): Dataset[ContactPerson] = {
    import spark.implicits._

    val exactMatches = updatedExactMatchInput.union(ingestedExactMatchInput)

    val fuzzyMatches = fuzzyMatchCombinedInputFile
      .join(exactMatches, Seq("concatId"), JoinType.LeftAnti)
      .as[ContactPerson]

    exactMatches.union(fuzzyMatches)
  }

  override def run(spark: SparkSession, config: ExactAndFuzzyMatchesConfig, storage: Storage): Unit = {
    import spark.implicits._

    log.info(s"Combining contact person exact match results from [${config.updatedExactMatchInputFile}] and " +
      s"[${config.ingestedExactMatchInputFile}] with fuzzy match results [${config.fuzzyMatchCombinedInputFile}] and write results to [${config.combinedOutputFile}]")

    val updatedExactMatchInput = storage.readFromParquet[ContactPerson](config.updatedExactMatchInputFile)
    val ingestedExactMatchInput = storage.readFromParquet[ContactPerson](config.ingestedExactMatchInputFile)
    val fuzzyMatchCombinedInputFile = storage.readFromParquet[ContactPerson](config.fuzzyMatchCombinedInputFile)

    val result: Dataset[ContactPerson] = transform(spark, updatedExactMatchInput, ingestedExactMatchInput, fuzzyMatchCombinedInputFile)

    storage.writeToParquet(result, config.combinedOutputFile)
  }
}
