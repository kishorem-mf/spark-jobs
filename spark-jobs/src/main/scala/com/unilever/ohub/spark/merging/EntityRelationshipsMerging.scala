package com.unilever.ohub.spark.merging

import java.util.UUID

import com.unilever.ohub.spark.{SparkJob, SparkJobConfig}
import com.unilever.ohub.spark.domain.entity.EntityRelationships
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{Dataset, SparkSession}
import scopt.OptionParser

case class EntityRelationshipsMergingConfig(
                                             entityRelationships: String = "EntityRelationships-input-file",
                                             previousIntegrated: String = "previous-integrated-file",
                                             outputFile: String = "path-to-output-file"
                                           ) extends SparkJobConfig

object EntityRelationshipsMerging extends SparkJob[EntityRelationshipsMergingConfig] {

  def transform(
                 spark: SparkSession,
                 entityRelationships: Dataset[EntityRelationships],
                 previousIntegrated: Dataset[EntityRelationships]
               ): Dataset[EntityRelationships] = {
    import spark.implicits._

    previousIntegrated
      .joinWith(entityRelationships, previousIntegrated("concatId") === entityRelationships("concatId"), JoinType.FullOuter)
      .map {
        case (integrated, entityRelationship) ⇒
          if (entityRelationship == null) {
            integrated
          } else {
            val ohubId = if (integrated == null) Some(UUID.randomUUID().toString) else integrated.ohubId

            entityRelationship.copy(ohubId = ohubId, isGoldenRecord = true)
          }
      }
  }

  override private[spark] def defaultConfig = EntityRelationshipsMergingConfig()

  override private[spark] def configParser(): OptionParser[EntityRelationshipsMergingConfig] =
    new scopt.OptionParser[EntityRelationshipsMergingConfig]("Entity Relationships merging") {
      head("merges Entity Relationships into an integrated Entity Relationships output file.", "1.0")
      opt[String]("Entity Relationships") required() action { (x, c) ⇒
        c.copy(entityRelationships = x)
      } text "Entity Relationships input file is a string property"
      opt[String]("previousIntegrated") required() action { (x, c) ⇒
        c.copy(previousIntegrated = x)
      } text "previousIntegrated is a string property"
      opt[String]("outputFile") required() action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"

      version("1.0")
      help("help") text "help text"
    }

  override def run(spark: SparkSession, config: EntityRelationshipsMergingConfig, storage: Storage): Unit = {
    log.info(
      s"Merging Entity Relationships from [${config.entityRelationships}] and [${config.previousIntegrated}] to [${config.outputFile}]"
    )

    val entityRelationship = storage.readFromParquet[EntityRelationships](config.entityRelationships)
    val previousIntegrated = storage.readFromParquet[EntityRelationships](config.previousIntegrated)
    val transformed = transform(spark, entityRelationship, previousIntegrated)

    storage.writeToParquet(transformed, config.outputFile)
  }
}
