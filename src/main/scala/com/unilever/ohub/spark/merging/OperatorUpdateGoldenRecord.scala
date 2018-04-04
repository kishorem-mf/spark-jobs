package com.unilever.ohub.spark.merging

import java.sql.Timestamp
import java.util.UUID

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.collect_list

object OperatorUpdateGoldenRecord extends SparkJob {


  def pickGoldenRecord(sourcePreference: Map[String, Int])
                                (operators: Seq[Operator]): Seq[Operator] = {
    val ohubId = operators.head.ohubId
    val goldenRecord = operators.reduce((o1, o2) => {
      val preference1 = sourcePreference.getOrElse(o1.sourceName, Int.MaxValue)
      val preference2 = sourcePreference.getOrElse(o2.sourceName, Int.MaxValue)
      if (preference1 < preference2) o1
      else if (preference1 > preference2) o2
      else { // same source preference
        val created1 = o1.dateCreated.getOrElse(new Timestamp(System.currentTimeMillis))
        val created2 = o1.dateCreated.getOrElse(new Timestamp(System.currentTimeMillis))
        if (created1.before(created2)) o1 else o2
      }
    })
    operators.map(o => o.copy(isGoldenRecord = (o == goldenRecord)))
  }

  def transform(
                 spark: SparkSession,
                 operators: Dataset[Operator],
                 sourcePreference: Map[String, Int]
               ): Dataset[Operator] = {
    import spark.implicits._

    operators
      .groupByKey(_.ohubId.get)
      .agg(collect_list("operator").alias("operators").as[Seq[Operator]])
      .map( _._2)
      .flatMap(pickGoldenRecord(sourcePreference))
      .repartition(60)
  }


  override val neededFilePaths = Array("MATCHING_INPUT_FILE", "OPERATOR_INPUT_FILE", "OUTPUT_FILE")

  override def run(spark: SparkSession, filePaths: Product, storage: Storage): Unit = {
    import spark.implicits._

    val (matchingInputFile: String, operatorInputFile: String, outputFile: String) = filePaths

    log.info(s"Merging operators from [$matchingInputFile] and [$operatorInputFile] to [$outputFile]")

    val operators = storage
      .readFromParquet[Operator](operatorInputFile)

    val transformed = transform(spark, operators, storage.sourcePreference)

    storage
      .writeToParquet(transformed, outputFile, partitionBy = Seq("countryCode"))
  }
}
