package com.unilever.ohub.spark.combining

import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

case class CombiningConfig(
    integratedUpdated: String = "path-to-integrated-updated-file",
    newGolden: String = "path-to-new-golden-file",
    newIntegratedOutput: String = "path-to-new integrated-file"
) extends SparkJobConfig

object OperatorCombining extends SparkJob[CombiningConfig] {

  def transform(
    spark: SparkSession,
    integratedUpdated: Dataset[Operator],
    newGoldenRecords: Dataset[Operator]
  ): Dataset[Operator] = {
    import spark.implicits._

    integratedUpdated
      .join(newGoldenRecords, Seq("concatId"), "left_anti")
      .as[Operator]
      .union(newGoldenRecords)
  }

  override private[spark] def defaultConfig = CombiningConfig()

  override private[spark] def configParser(): OptionParser[CombiningConfig] =
    new scopt.OptionParser[CombiningConfig]("Operator combiner") {
      head("combines operators from integrated and new golden into new integrated output.", "1.0")
      opt[String]("integratedUpdated") required () action { (x, c) ⇒
        c.copy(integratedUpdated = x)
      } text "integratedUpdated is a string property"
      opt[String]("newGolden") required () action { (x, c) ⇒
        c.copy(newGolden = x)
      } text "newGolden is a string property"
      opt[String]("newIntegratedOutput") required () action { (x, c) ⇒
        c.copy(combinedOperators = x)
      } text "newIntegratedOutput is a string property"

      version("1.0")
      help("help") text "help text"
    }

  override def run(spark: SparkSession, config: CombiningConfig, storage: Storage): Unit = {
    import spark.implicits._

    val integratedMatched = storage.readFromParquet[Operator](config.integratedUpdated)
    val newGolden = storage.readFromParquet[Operator](config.newGolden)
    val newIntegrated = transform(spark, integratedMatched, newGolden)

    storage.writeToParquet(newIntegrated, config.newIntegratedOutput)
  }
}
