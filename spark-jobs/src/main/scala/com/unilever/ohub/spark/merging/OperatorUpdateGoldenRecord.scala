package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{ DefaultConfig, DomainDataProvider, SparkJobWithDefaultDbConfig }
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object OperatorUpdateGoldenRecord extends SparkJobWithDefaultDbConfig with GoldenRecordPicking[Operator] {
  val DEFAULT_CHECKPOINT_DIR = "/checkpoints"

  def markGoldenRecord(sourcePreference: Map[String, Int])(operators: Seq[Operator]): Seq[Operator] = {
    val goldenRecord = pickGoldenRecord(sourcePreference, operators)
    operators.map(o ⇒ o.copy(isGoldenRecord = o == goldenRecord))
  }

  def transform(
    spark: SparkSession,
    operators: Dataset[Operator],
    sourcePreference: Map[String, Int],
    sizeThreshold: Int = 250000
  ): Dataset[Operator] = {
    import spark.implicits._

    val ohubIdHavingEnormousGroups: Array[String] = operators
      .groupBy("ohubId")
      .count
      .filter($"count" >= sizeThreshold)
      .select("ohubId")
      .as[String]
      .collect()

    // original way of selecting golden records using source preferences
    val goldenRecordCorrect = operators
      .map(x ⇒ x.ohubId.get -> x)
      .toDF("ohubId", "operator")
      .filter($"ohubId".isin(ohubIdHavingEnormousGroups: _*) === false)
      .groupBy("ohubId")
      .agg(collect_list($"operator").as("operators"))
      .select("operators")
      .as[Seq[Operator]]
      .flatMap(markGoldenRecord(sourcePreference))
      .checkpoint // checkpoints are needed to remove the lineage of the dataset, otherwise it causes a diamond problem on the unionByName

    // spark will run out of memory, due to the flatmap and some partitions/groups having over 1M records
    // this will select the golden records by picking the very first record per group, sorted arbitrarily, avoiding the out of memeory issue
    val w = Window.partitionBy($"ohubId").orderBy($"concatId")
    val goldenRecordCheap = operators
      .filter($"ohubId".isin(ohubIdHavingEnormousGroups: _*) === true)
      .withColumn("rn", row_number().over(w))
      .withColumn("isGoldenRecord", when($"rn" === 1, true).otherwise(false))
      .drop("rn")
      .as[Operator]
      .checkpoint // checkpoints are needed to remove the lineage of the dataset, otherwise it causes a diamond problem on the unionByName

    goldenRecordCorrect.unionByName(goldenRecordCheap)
  }

  override def run(spark: SparkSession, config: DefaultConfig, storage: Storage): Unit = {
    run(spark, config, storage, DomainDataProvider(spark))
  }

  protected[merging] def run(spark: SparkSession, config: DefaultConfig, storage: Storage, dataProvider: DomainDataProvider): Unit = {
    val operators = storage.readFromParquet[Operator](config.inputFile)

    spark.sparkContext.getCheckpointDir match {
      case Some(dir) ⇒ log.info(s"checkpoint directory already set to $dir")
      case None ⇒
        log.info(s"checkpoint directory not set, setting to $DEFAULT_CHECKPOINT_DIR")
        spark.sparkContext.setCheckpointDir(DEFAULT_CHECKPOINT_DIR)
    }

    val transformed = transform(spark, operators, dataProvider.sourcePreferences)
    storage.writeToParquet(transformed, config.outputFile)
  }
}
