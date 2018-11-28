package com.unilever.ohub.spark.combining

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

import scala.reflect.runtime.universe._

case class CombiningConfig(
    integratedUpdated: String = "path-to-integrated-updated-file",
    newGolden: String = "path-to-new-golden-file",
    combinedEntities: String = "path-to-new combined entities"
) extends SparkJobConfig

abstract class BaseCombining[T <: DomainEntity: TypeTag] extends SparkJob[CombiningConfig] {

  def transform(
    spark: SparkSession,
    integratedUpdated: Dataset[T],
    newGoldenRecords: Dataset[T]
  ): Dataset[T] = {
    import spark.implicits._

    integratedUpdated
      .join(newGoldenRecords, Seq("concatId"), "left_anti")
      .as[T]
      .map(identity)
      .union(newGoldenRecords)
  }

  override private[spark] def defaultConfig = CombiningConfig()

  override private[spark] def configParser(): OptionParser[CombiningConfig] =
    new scopt.OptionParser[CombiningConfig]("Domain entity combiner") {
      head("combines entities from integrated and new golden into new integrated output.", "1.0")
      opt[String]("integratedUpdated") required () action { (x, c) ⇒
        c.copy(integratedUpdated = x)
      } text "integratedUpdated is a string property"
      opt[String]("newGolden") required () action { (x, c) ⇒
        c.copy(newGolden = x)
      } text "newGolden is a string property"
      opt[String]("combinedEntities") required () action { (x, c) ⇒
        c.copy(combinedEntities = x)
      } text "combinedEntities is a string property"

      version("1.0")
      help("help") text "help text"
    }

  override def run(spark: SparkSession, config: CombiningConfig, storage: Storage): Unit = {
    val integratedMatched = storage.readFromParquet[T](config.integratedUpdated)
    val newGolden = storage.readFromParquet[T](config.newGolden)
    val newIntegrated = transform(spark, integratedMatched, newGolden)

    storage.writeToParquet(newIntegrated, config.combinedEntities)
  }
}
