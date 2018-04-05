package com.unilever.ohub.spark.merging

import java.util.UUID

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.apache.spark.sql.functions.collect_list

object OperatorMerging extends SparkJob with OperatorGoldenRecord {

  private case class MatchingResult(sourceId: String, targetId: String, countryCode: String)

  private case class IdAndCountry(concatId: String, countryCode: String)

  private case class MatchingResultAndOperator(
      matchingResult: MatchingResult,
      operator: Operator
  ) {
    val sourceId: String = matchingResult.sourceId
  }

  def markGoldenRecordAndGroupId(sourcePreference: Map[String, Int])(operators: Seq[Operator]): Seq[Operator] = {
    val goldenRecord = pickGoldenRecord(sourcePreference, operators)
    val groupId = UUID.randomUUID().toString
    operators.map(o ⇒ o.copy(ohubId = Some(groupId), isGoldenRecord = (o == goldenRecord)))
  }

  def transform(
    spark: SparkSession,
    operators: Dataset[Operator],
    matches: Dataset[MatchingResult],
    sourcePreference: Map[String, Int]
  ): Dataset[Operator] = {
    import spark.implicits._

    val groupedOperators = matches
      .joinWith(
        operators,
        matches("countryCode") === operators("countryCode")
          and $"targetId" === $"concatId"
      )
      .map((MatchingResultAndOperator.apply _).tupled)
      .groupByKey(_.sourceId)
      .agg(collect_list("operator").alias("operators").as[Seq[Operator]])
      .joinWith(operators, $"value" === $"concatId")
      .map(x ⇒ x._2 +: x._1._2)

    val matchedIds = groupedOperators
      .flatMap(_.map(x ⇒ IdAndCountry(x.concatId, x.countryCode)))
      .distinct()

    val singletonOperators = operators
      .join(matchedIds, Seq("concatId"), JoinType.LeftAnti)
      .as[Operator]
      .map(Seq(_))

    groupedOperators
      .union(singletonOperators)
      .flatMap(markGoldenRecordAndGroupId(sourcePreference))
      .repartition(60)
  }

  override val neededFilePaths = Array("MATCHING_INPUT_FILE", "OPERATOR_INPUT_FILE", "OUTPUT_FILE")

  override def run(spark: SparkSession, filePaths: Product, storage: Storage): Unit = {
    import spark.implicits._

    val (matchingInputFile: String, operatorInputFile: String, outputFile: String) = filePaths

    log.info(s"Merging operators from [$matchingInputFile] and [$operatorInputFile] to [$outputFile]")

    val operators = storage
      .readFromParquet[Operator](operatorInputFile)

    val matches = storage
      .readFromParquet[MatchingResult](
        matchingInputFile,
        selectColumns = Seq(
          $"sourceId",
          $"targetId",
          $"countryCode"
        )
      )

    val transformed = transform(spark, operators, matches, storage.sourcePreference)

    storage
      .writeToParquet(transformed, outputFile, partitionBy = Seq("countryCode"))
  }
}
