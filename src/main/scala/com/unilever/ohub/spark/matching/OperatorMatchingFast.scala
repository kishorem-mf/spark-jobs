package com.unilever.ohub.spark.matching

import com.unilever.ohub.spark.generic.StringFunctions
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.SparkSession

object OperatorMatchingFast extends App {

  if (args.length != 3) {
    println("specify INPUT_FILE OUTPUT_FOLDER HELP_FILE")
    sys.exit(1)
  }

  val inputFile = args(0)
  val outputFolder = args(1)
  val helpFile = args(2)

  val THRESHOLD = 0.86

  val spark = SparkSession
    .builder()
    .appName("Operator matching")
    .getOrCreate()

  import spark.implicits._

  spark.sqlContext.udf.register("SIMILARITY", (s1: String, s2: String) => StringFunctions.getFastSimilarity(s1 match { case null => null; case _ => s1.toCharArray }, s2 match { case null => null; case _ => s2.toCharArray }))

  val preMatchDF = spark.read.parquet(helpFile).toDF(Seq("matched_string","country_code","source_id","target_id","similarity","source_name","target_name"): _*)
  preMatchDF.createOrReplaceTempView("pre_match")
  val optionalColumns = """,regexp_replace(lower(source_name),"unknown","") source_name,regexp_replace(lower(target_name),"unknown","") target_name,similarity(source_name,target_name) similarity"""

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

  println("Done")
}
