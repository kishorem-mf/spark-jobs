package com.unilever.ohub.spark.dispatcher

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.export.DeltaFunctions
import com.unilever.ohub.spark.dispatcher.model.DispatcherContactPerson
import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

object ContactPersonDispatcherConverter extends SparkJob[DefaultWithDbAndDeltaConfig]
  with DeltaFunctions {

  /**
   * A process that transforms a source [[com.unilever.ohub.spark.domain.entity.ContactPerson]] to a
   * destination [[com.unilever.ohub.spark.dispatcher.model.DispatcherContactPerson]] according to the following logic:
   * <ul>
   *   <li>Read the input file containing ContactPerson records</li>
   *   <li>Read an optional previous-run file containing ContactPerson records, and if present, integrate the input and previous file</li>
   *   <li>Transform the ContactPerson records to the [[com.unilever.ohub.spark.dispatcher.model.DispatcherContactPerson]]</li>
   * </ul>
   * @param spark EntryPoint to the Spark Platform
   * @param contactPersons Source [[com.unilever.ohub.spark.domain.entity.ContactPerson]]
   * @param previousIntegrated previous [[com.unilever.ohub.spark.domain.entity.ContactPerson]]
   * @return A Spark Dataset containing the transformed [[com.unilever.ohub.spark.dispatcher.model.DispatcherContactPerson]]
   */
  def transform(
    spark: SparkSession,
    contactPersons: Dataset[ContactPerson],
    previousIntegrated: Dataset[ContactPerson]
  ): Dataset[DispatcherContactPerson] = {
    val dailyUfsContactPersons = createUfsContactPersons(spark, contactPersons)
    val allPreviousUfsContactPersons = createUfsContactPersons(spark, previousIntegrated)

    integrate[DispatcherContactPerson](spark, dailyUfsContactPersons, allPreviousUfsContactPersons, "CP_ORIG_INTEGRATION_ID")
  }

  override private[spark] def defaultConfig = DefaultWithDbAndDeltaConfig()

  override private[spark] def configParser(): OptionParser[DefaultWithDbAndDeltaConfig] = DefaultWithDbAndDeltaConfigParser()

  override def run(spark: SparkSession, config: DefaultWithDbAndDeltaConfig, storage: Storage): Unit = {
    import spark.implicits._

    log.info(s"Generating contact person Dispatcher csv file from [$config.inputFile] to [$config.outputFile]")

    val contactPersons = storage.readFromParquet[ContactPerson](config.inputFile)
    val previousIntegrated = config.previousIntegrated match {
      case Some(s) ⇒ storage.readFromParquet[ContactPerson](s)
      case None ⇒
        log.warn(s"No existing integrated file specified -- regarding as initial load.")
        spark.emptyDataset[ContactPerson]
    }
    val transformed: Dataset[DispatcherContactPerson] = transform(spark, contactPersons, previousIntegrated)

    storage.writeToSingleCsv(
      ds = transformed,
      outputFile = config.outputFile,
      options = EXTRA_WRITE_OPTIONS
    )
  }

  /**
   * Map a [[com.unilever.ohub.spark.domain.entity.ContactPerson]] to a [[com.unilever.ohub.spark.dispatcher.ContactPersonDispatcherConverter]]
   * @param spark EntryPoint to the Spark Platform
   * @param contactPersons A dataset of DispatcherContactPerson
   * @return A dataset of DispatcherContactPerson
   */
  def createUfsContactPersons(
    spark: SparkSession,
    contactPersons: Dataset[ContactPerson]
  ): Dataset[DispatcherContactPerson] = {
    import spark.implicits._

    contactPersons
      .filter(_.isGoldenRecord)
      .map(DispatcherContactPerson.fromContactPerson)
  }
}
