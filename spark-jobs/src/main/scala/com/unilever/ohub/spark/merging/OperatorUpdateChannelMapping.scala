package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.domain.entity.{ ChannelMapping, ChannelReference, Operator }
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{ DomainDataProvider, SparkJob, SparkJobConfig }
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

case class OperatorUpdateChannelMappingConfig(
    channelMappingsInputFile: String = "channel-mappings-input-file",
    operatorsInputFile: String = "operators-input-file",
    outputFile: String = "path-to-output-file"
) extends SparkJobConfig

object OperatorUpdateChannelMapping extends SparkJob[OperatorUpdateChannelMappingConfig] {

  override def defaultConfig = OperatorUpdateChannelMappingConfig()

  def transform(
    spark: SparkSession,
    operators: Dataset[Operator],
    channelMappings: Dataset[ChannelMapping],
    channelReferences: Map[String, ChannelReference]
  ): Dataset[Operator] = {
    import spark.implicits._

    operators.as("op")
      .joinWith(channelMappings.as("cm"), $"op.countryCode" === $"cm.countryCode" && $"op.channel" === $"cm.originalChannel", JoinType.LeftOuter)
      .map {
        case (operator: Operator, channelMapping: ChannelMapping) ⇒ {
          val channelReference = channelReferences.getOrElse(
            channelMapping.channelReference,
            channelReferences.get(ChannelReference.unknownChannelReferenceId).get
          )

          operator.copy(
            localChannel = Some(channelMapping.localChannel),
            channelUsage = Some(channelMapping.channelUsage),
            socialCommercial = channelReference.socialCommercial,
            strategicChannel = Some(channelReference.strategicChannel),
            globalChannel = Some(channelReference.globalChannel),
            globalSubChannel = Some(channelReference.globalSubChannel)
          )
        }
        case (operator: Operator, _) ⇒ operator
      }.as[Operator]
  }

  override private[spark] def configParser(): OptionParser[OperatorUpdateChannelMappingConfig] =
    new scopt.OptionParser[OperatorUpdateChannelMappingConfig]("Subscriptions merging") {
      head("enriches operators with channelMapping data", "1.0")
      opt[String]("channelMappingsInputFile") required () action { (x, c) ⇒
        c.copy(channelMappingsInputFile = x)
      } text "channelMappingsInputFile is a string property"
      opt[String]("operatorsInputFile") required () action { (x, c) ⇒
        c.copy(operatorsInputFile = x)
      } text "operatorsInputFile is a string property"
      opt[String]("outputFile") optional () action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"

      version("1.0")
      help("help") text "help text"
    }

  override def run(spark: SparkSession, config: OperatorUpdateChannelMappingConfig, storage: Storage): Unit = {
    val dataProvider = DomainDataProvider(spark)
    val operators = storage.readFromParquet[Operator](config.operatorsInputFile)
    val channelMappings = storage.readFromParquet[ChannelMapping](config.channelMappingsInputFile)
    val transformed = transform(spark, operators, channelMappings, dataProvider.channelReferences)

    storage.writeToParquet(transformed, config.outputFile)
  }
}
