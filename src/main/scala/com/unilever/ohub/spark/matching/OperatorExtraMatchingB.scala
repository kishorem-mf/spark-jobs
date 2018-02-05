package com.unilever.ohub.spark.matching

import com.unilever.ohub.spark.generic.{ FileSystems, StringFunctions }
import org.apache.log4j.{ LogManager, Logger }
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.SparkSession

object OperatorExtraMatchingB extends App {
  implicit private val log: Logger = LogManager.getLogger(this.getClass)

  val (inputFile, outputFolder, helpFile) = FileSystems.getFileNames(
    args,
    "INPUT_FILE", "OUTPUT_FOLDER", "HELP_FILE"
  )

  val THRESHOLD = 0.86

  val spark = SparkSession
    .builder()
    .appName("Operator matching")
    .getOrCreate()

  spark
    .sqlContext
    .udf
    .register("SIMILARITY", (s1: String, s2: String) => {
      StringFunctions.getFastSimilarity(s1.toCharArray, s2.toCharArray)
    })

  val preMatchDF = spark
    .read
    .parquet(helpFile)
    .toDF(
      "matched_string",
      "country_code",
      "source_id",
      "target_id",
      "similarity",
      "source_name",
      "target_name"
    )
  preMatchDF.createOrReplaceTempView("pre_match")

  val matchIdDF = spark.sql(
    s"""
      |select country_code,min(source_id) source_id,target_id target_id
      |from pre_match
      |where 1 = 1
      | and similarity(regexp_replace(lower(source_name),"unknown",""),regexp_replace(lower(target_name),"unknown","")) > $THRESHOLD
      | and similarity > $THRESHOLD
      | and source_id < target_id
      |group by country_code,target_id
      |order by country_code,target_id
    """.stripMargin).where("country_code = 'DK'")
  matchIdDF.createOrReplaceTempView("match_id")
  val finalMatchDF = spark.sql(
    """
      |select pre_match.country_code,pre_match.source_id,pre_match.target_id,pre_match.similarity,pre_match.source_name,pre_match.target_name
      |from match_id
      |inner join pre_match
      | on match_id.country_code = pre_match.country_code
      | and match_id.source_id = pre_match.source_id
      | and match_id.target_id = pre_match.target_id
    """.stripMargin)
  finalMatchDF.write.mode(Overwrite).parquet(s"$outputFolder.parquet")

  log.info("Done")
}
