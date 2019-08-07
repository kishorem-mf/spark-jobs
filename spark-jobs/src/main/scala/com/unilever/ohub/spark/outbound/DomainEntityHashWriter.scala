package com.unilever.ohub.spark.outbound

import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import com.unilever.ohub.spark.domain.entity._
import com.unilever.ohub.spark.domain.{ DomainEntity, DomainEntityHash }
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

import scala.reflect.runtime.universe._

case class HashWriterConfig(
    integratedInputFile: String = "integrated-input-file",
    previousHashFile: Option[String] = None,
    hashesOutputFile: String = "hashes-output-file"
) extends SparkJobConfig

object OperatorHashWriter extends DomainEntityHashWriter[Operator]

object ContactPersonHashWriter extends DomainEntityHashWriter[ContactPerson]

object SubscriptionHashWriter extends DomainEntityHashWriter[Subscription]

object ProductHashWriter extends DomainEntityHashWriter[Product]

object OrderHashWriter extends DomainEntityHashWriter[Order]

object OrderLineHashWriter extends DomainEntityHashWriter[OrderLine]

object ActivityHashWriter extends DomainEntityHashWriter[Activity]

object QuestionHashWriter extends DomainEntityHashWriter[Question]

object AnswerHashWriter extends DomainEntityHashWriter[Answer]

object LoyaltyPointsHashWriter extends DomainEntityHashWriter[LoyaltyPoints]

object CampaignHashWriter extends DomainEntityHashWriter[Campaign]

object CampaignSendHashWriter extends DomainEntityHashWriter[CampaignSend]

object CampaignOpenHashWriter extends DomainEntityHashWriter[CampaignOpen]

object CampaignBounceHashWriter extends DomainEntityHashWriter[CampaignBounce]

object CampaignClickHashWriter extends DomainEntityHashWriter[CampaignClick]

object ChainHashWriter extends DomainEntityHashWriter[Chain]

abstract class DomainEntityHashWriter[DomainType <: DomainEntity: TypeTag] extends SparkJob[HashWriterConfig] {

  override private[spark] def defaultConfig = HashWriterConfig()

  override private[spark] def configParser(): OptionParser[HashWriterConfig] =
    new scopt.OptionParser[HashWriterConfig]("Spark job default") {
      head("run a spark job with default config.", "1.0")

      opt[String]("integratedInputFile") required () action { (x, c) ⇒
        c.copy(integratedInputFile = x)
      } text "integratedInputFile is a string property"
      opt[String]("previousHashFile") optional () action { (x, c) ⇒
        c.copy(previousHashFile = Some(x))
      } text "previousHashFile is a string property"
      opt[String]("hashesOutputFile") required () action { (x, c) ⇒
        c.copy(hashesOutputFile = x)
      } text "hashesOutputFile is a string property"

      version("1.0")
      help("help") text "help text"
    }

  def determineHashes(spark: SparkSession, integratedEntities: Dataset[DomainType], previousHashes: Dataset[DomainEntityHash]): Dataset[DomainEntityHash] = {
    import spark.implicits._

    val md5Hash: String ⇒ String = input ⇒ {
      import java.security.MessageDigest
      import java.math.BigInteger
      val md = MessageDigest.getInstance("MD5")
      val digest = md.digest(input.getBytes)
      val bigInt = new BigInteger(1, digest)
      val hashedString = bigInt.toString(16)
      hashedString
    }

    integratedEntities
      .drop("id", "creationTimestamp", "ohubCreated", "ohubUpdated") // excluded columns from hash
      .map(ie ⇒ (ie.getAs[String]("concatId"), md5Hash(ie.toString)))
      .toDF("concatId", "newHash")
      .join(previousHashes, Seq("concatId"), JoinType.LeftOuter)
      .withColumn("md5Hash", when('md5Hash.isNull, "no-hash").otherwise('md5Hash))
      .withColumn("hasChanged", 'newHash =!= 'md5Hash) // check whether the hash has changed
      .withColumn("md5Hash", 'newHash) // new hash is current md5 hash
      .select('concatId, 'hasChanged, 'md5Hash)
      .as[DomainEntityHash]
  }

  def run(spark: SparkSession, config: HashWriterConfig, storage: Storage): Unit = {
    import spark.implicits._

    log.info(s"Determining hashes for '${config.integratedInputFile}'")

    val integratedEntities = storage.readFromParquet[DomainType](config.integratedInputFile)
    val previousHashes = config.previousHashFile match {
      case Some(location) ⇒ storage.readFromParquet[DomainEntityHash](location)
      case None           ⇒ spark.createDataset(Seq[DomainEntityHash]()) // no previous hash file configured -> everything marked as changed
    }
    val currentHashes = determineHashes(spark, integratedEntities, previousHashes)
    storage.writeToParquet(currentHashes, config.hashesOutputFile)
  }
}
