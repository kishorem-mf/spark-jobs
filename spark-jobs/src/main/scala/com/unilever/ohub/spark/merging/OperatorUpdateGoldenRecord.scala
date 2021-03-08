package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{DomainDataProvider, SparkJob, SparkJobConfig}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}
import scopt.OptionParser

case class OperatorUpdateGoldenRecordConfig(
                                             inputFile: String = "path-to-input-file",
                                             checkpointFile: String = "path-to-checkpoint-file",
                                             outputFile: String = "path-to-output-file"
                                           ) extends SparkJobConfig
object OperatorUpdateGoldenRecord extends SparkJob[OperatorUpdateGoldenRecordConfig] with GoldenRecordPicking[Operator] {
  def markGoldenRecord(sourcePreference: Map[String, Int])(operators: Seq[Operator]): Seq[Operator] = {
    val goldenRecord = pickGoldenRecord(sourcePreference, operators)
    operators.map(o ⇒ o.copy(isGoldenRecord = o == goldenRecord))
  }

  def transform(
                 spark: SparkSession,
                 operators: Dataset[Operator],
                 sourcePreference: Map[String, Int],
                 sizeThreshold: Int = 250000 // scalastyle:ignore
               ): Dataset[Operator] = {
    import spark.implicits._
    val ohubIdHavingEnormousGroups: Array[String] = operators.filter(!$"ohubId".isNull) // We are temporarily handling the exclusion of null ohubId to avoid errors in overnight processing
      .groupBy("ohubId")
      .count
      .filter($"count" >= sizeThreshold)
      .select("ohubId")
      .as[String]
      .collect()

    // original way of selecting golden records using source preferences
    // We are temporarily handling the exclusion of null ohubId to avoid errors in overnight processing
    // TODO: Got to ensure there is no null ohubid before this stage.
    // When oldintegrationid is passed with some value which is not in the prevIntegrated, then we get NULL OHUBID
    // We need to ensure new ohubid gets generated when there is no match of oldintegrationId in prevIntegrated
    val goldenRecordCorrect = operators.filter(!$"ohubId".isNull)
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
    val goldenRecordCheap = operators.filter(!$"ohubId".isNull) // We are temporarily handling the exclusion of null ohubId to avoid errors in overnight processing
      .filter($"ohubId".isin(ohubIdHavingEnormousGroups: _*) === true)
      .withColumn("rn", row_number().over(w))
      .withColumn("isGoldenRecord", when($"rn" === 1, true).otherwise(false))
      .drop("rn")
      .as[Operator]
      .checkpoint // checkpoints are needed to remove the lineage of the dataset, otherwise it causes a diamond problem on the unionByName

    goldenRecordCorrect.unionByName(goldenRecordCheap)
  }

  override private[spark] def defaultConfig = OperatorUpdateGoldenRecordConfig()

  override private[spark] def configParser(): OptionParser[OperatorUpdateGoldenRecordConfig] =
    new scopt.OptionParser[OperatorUpdateGoldenRecordConfig]("Question merging") {
      head("merges questions into an integrated questions output file.", "1.0")
      opt[String]("inputFile") required() action { (x, c) ⇒
        c.copy(inputFile = x)
      } text "inputFile is a string property"
      opt[String]("checkpointFile") required() action { (x, c) ⇒
        c.copy(checkpointFile = x)
      } text "checkpointFile is a string property"
      opt[String]("outputFile") required() action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"

      version("1.0")
      help("help") text "help text"
    }

  override def run(spark: SparkSession, config: OperatorUpdateGoldenRecordConfig, storage: Storage): Unit = {
    run(spark, config, storage, DomainDataProvider())
  }

  protected[merging] def run(spark: SparkSession, config: OperatorUpdateGoldenRecordConfig, storage: Storage, dataProvider: DomainDataProvider): Unit = {
    val operators = storage.readFromParquet[Operator](config.inputFile)
    spark.sparkContext.setCheckpointDir(config.checkpointFile)

    val transformed = transform(spark, operators, dataProvider.sourcePreferences)
    storage.writeToParquet(transformed, config.outputFile)
  }
}
