package com.unilever.ohub.spark.status

import com.unilever.ohub.spark.domain.{DomainEntity, DomainEntityCompanion, DomainEntityUtils}
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{SparkJob, SparkJobConfig}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

case class AllEntityParquetsSuccessfulConfig(
    basePath: String = "basepath",
    runId: String = "run-id"
) extends SparkJobConfig

object AllEntityParquetsSuccessful extends SparkJob[AllEntityParquetsSuccessfulConfig] {
  override private[spark] def defaultConfig = AllEntityParquetsSuccessfulConfig()

  override private[spark] def configParser() = new scopt.OptionParser[AllEntityParquetsSuccessfulConfig]("Activity merging") {
    head("Checks if success files are present for all entities starting from provided basepath. If not, an exception will be thrown.", "1.0")
    opt[String]("basePath") required () action { (x, c) ⇒
      c.copy(basePath = x)
    } text "basePath is a string property"
    opt[String]("runId") required () action { (x, c) ⇒
      c.copy(runId = x)
    } text "runId is a string property"
    version("1.0")
    help("help") text "help text"
  }

  private def successFileExists(location: String)(implicit spark: SparkSession): Boolean = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val exists = fs.exists(new Path(location, "_SUCCESS"))
    if(exists) log.info(s"_SUCCESS file found in ${location}") else log.info(s"No _SUCCESS file found in ${location}")
    exists
  }

  def checkAllSuccessFiles(basePath: String, runId: String)(implicit spark: SparkSession): Unit = {
    val allDomainCompanions = DomainEntityUtils.domainCompanionObjects
    val unsuccessfulDomains = allDomainCompanions
      .filter((domainCompanion: DomainEntityCompanion[_ <: DomainEntity]) ⇒ !successFileExists(s"${basePath}/${runId}/${domainCompanion.engineFolderName}.parquet"))

    unsuccessfulDomains.length > 0 match {
      case true  ⇒ {
        throw new NotAllEntitesSuccessfulException(s"Entities without success file: ${unsuccessfulDomains.map(_.getClass.getSimpleName).mkString(", ")}.")
      }
      case false ⇒ log.info("All success files present")
    }
  }

  override def run(spark: SparkSession, config: AllEntityParquetsSuccessfulConfig, storage: Storage): Unit =
    checkAllSuccessFiles(config.basePath, config.runId)(spark)

}

class NotAllEntitesSuccessfulException(message: String) extends RuntimeException(message)
