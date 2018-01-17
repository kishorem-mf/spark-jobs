package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.tsv2parquet.OperatorRecord
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.collect_list

case class OperatorWrapper(id:String, operator:OperatorRecord)

object OperatorMerging extends App {

  if (args.length != 3) {
    println("specify MATCHING_INPUT_FILE OPERATOR_INPUT_FILE OUTPUT_FILE")
    sys.exit(1)
  }

  val matchingInputFile = args(0)
  val operatorInputFile = args(1)
  val outputFile = args(2)

  println(s"Merging operators from [$matchingInputFile] and [$operatorInputFile] to [$outputFile]")

  val spark = SparkSession
    .builder()
    .appName(this.getClass.getSimpleName)
    .getOrCreate()

  import spark.implicits._

  val operators:Dataset[OperatorWrapper] = spark.read.parquet(operatorInputFile)
    .filter("COUNTRY_CODE = 'DK'")
    .as[OperatorRecord]
    .map(o => OperatorWrapper(id = s"${o.COUNTRY_CODE.get}~${o.SOURCE.get}~${o.REF_OPERATOR_ID.get}", operator = o))

  val matches = spark.read.parquet(matchingInputFile).select("source_id", "target_id")

  // join the full operators to the target_id side of the matches
  val targetIdJoined = matches.join(operators.select($"id".alias("target_id"), $"operator"), "target_id")
    .select("source_id", "operator")

  // group up the operators based on the source id
  val groupedBySource = targetIdJoined.groupBy("source_id").agg(collect_list("operator").alias("operators"))

  // fetch the operator that goes with the source id and stick it in the group with the operators from the groupBy
  val fullyGroupedOperators:Dataset[Seq[OperatorRecord]] = groupedBySource.join(operators.select($"id".alias("source_id"), $"operator"), "source_id")
    .select($"operator".as[OperatorRecord], $"operators".as[Seq[OperatorRecord]])
    .map(x => x._1 +: x._2)

  fullyGroupedOperators.write.mode(Overwrite).format("parquet").save(outputFile)

  println("Done")
}
