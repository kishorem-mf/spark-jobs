package com.unilever.ohub.spark.acm

import java.io.File
import java.util.UUID

import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import com.unilever.ohub.spark.data.ChannelMapping
import com.unilever.ohub.spark.acm.model.UFSOperator
import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

case class DefaultWithDbConfig(
    inputFile: String = "path-to-input-file",
    outputFile: String = "path-to-output-file",
    postgressUrl: String = "postgress-url",
    postgressUsername: String = "postgress-username",
    postgressPassword: String = "postgress-password",
    postgressDB: String = "postgress-db"
) extends SparkJobConfig {
  val filePath = new Path(outputFile)
  val temporaryPath = new Path(filePath.getParent, UUID.randomUUID().toString)
}

object OperatorAcmConverter extends SparkJob[DefaultWithDbConfig] with AcmTransformationFunctions {

  def transform(
    spark: SparkSession,
    channelMappings: Dataset[ChannelMapping],
    operators: Dataset[Operator]
  ): Dataset[UFSOperator] = {
    import spark.implicits._

    val ufsOperators = operators
      .filter(_.isGoldenRecord)
      .map(operator ⇒

        UFSOperator(
          OPR_ORIG_INTEGRATION_ID = operator.ohubId.getOrElse("UNKNOWN"),
          OPR_LNKD_INTEGRATION_ID = operator.concatId,
          GOLDEN_RECORD_FLAG = boolAsString(operator.isGoldenRecord),
          COUNTRY_CODE = operator.countryCode,
          NAME = clean(operator.name),
          CHANNEL = operator.channel,
          SUB_CHANNEL = operator.subChannel,
          ROUTE_TO_MARKET = "",
          REGION = operator.region,
          OTM = operator.otm,
          PREFERRED_PARTNER = operator.distributorName.map(clean),
          STREET = operator.street,
          HOUSE_NUMBER = operator.houseNumber,
          ZIPCODE = operator.zipCode,
          CITY = operator.city.map(clean),
          COUNTRY = operator.countryName,
          AVERAGE_SELLING_PRICE = operator.averagePrice.map(_.setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble),
          NUMBER_OF_COVERS = operator.totalDishes,
          NUMBER_OF_WEEKS_OPEN = operator.weeksClosed.map { weeksClosed ⇒
            if (52 - weeksClosed < 0) 0 else 52 - weeksClosed
          },
          NUMBER_OF_DAYS_OPEN = operator.daysOpen,
          CONVENIENCE_LEVEL = operator.cookingConvenienceLevel,
          RESPONSIBLE_EMPLOYEE = operator.salesRepresentative,
          NPS_POTENTIAL = operator.netPromoterScore,
          CAM_KEY = "",
          CAM_TEXT = "",
          CHANNEL_KEY = "",
          CHANNEL_TEXT = "",
          CHAIN_KNOTEN = operator.chainId,
          CHAIN_NAME = operator.chainName.map(clean),
          CUST_SUB_SEG_EXT = "",
          CUST_SEG_EXT = "",
          CUST_SEG_KEY_EXT = "",
          CUST_GRP_EXT = "",
          PARENT_SEGMENT = "",
          DATE_CREATED = operator.dateCreated,
          DATE_UPDATED = operator.dateUpdated,
          DELETE_FLAG = if (operator.isActive) "N" else "Y",
          WHOLESALER_OPERATOR_ID = operator.distributorOperatorId,
          PRIVATE_HOUSEHOLD = operator.isPrivateHousehold.map(boolAsString),
          VAT = operator.vat,
          OPEN_ON_MONDAY = operator.isOpenOnMonday.map(boolAsString),
          OPEN_ON_TUESDAY = operator.isOpenOnTuesday.map(boolAsString),
          OPEN_ON_WEDNESDAY = operator.isOpenOnWednesday.map(boolAsString),
          OPEN_ON_THURSDAY = operator.isOpenOnThursday.map(boolAsString),
          OPEN_ON_FRIDAY = operator.isOpenOnFriday.map(boolAsString),
          OPEN_ON_SATURDAY = operator.isOpenOnSaturday.map(boolAsString),
          OPEN_ON_SUNDAY = operator.isOpenOnSunday.map(boolAsString),
          KITCHEN_TYPE = operator.kitchenType.map(clean)
        )
      )

    ufsOperators
      .joinWith(
        channelMappings,
        channelMappings("originalChannel") === ufsOperators("CHANNEL") and
          channelMappings("countryCode") === ufsOperators("COUNTRY_CODE"),
        JoinType.Left
      )
      .map {
        case (operator, maybeChannelMapping) ⇒ Option(maybeChannelMapping).fold(operator) { channelMapping ⇒
          operator.copy(
            LOCAL_CHANNEL = Option(channelMapping.localChannel),
            CHANNEL_USAGE = Option(channelMapping.channelUsage),
            SOCIAL_COMMERCIAL = Option(channelMapping.socialCommercial),
            STRATEGIC_CHANNEL = Option(channelMapping.strategicChannel),
            GLOBAL_CHANNEL = Option(channelMapping.globalChannel),
            GLOBAL_SUBCHANNEL = Option(channelMapping.globalSubChannel)
          )
        }
      }
  }

  override private[spark] def defaultConfig = DefaultWithDbConfig()

  override private[spark] def configParser(): OptionParser[DefaultWithDbConfig] =
    new scopt.OptionParser[DefaultWithDbConfig]("Operator ACM converter") {
      head("converts domain operators into ufs operators.", "1.0")
      opt[String]("inputFile") required () action { (x, c) ⇒
        c.copy(inputFile = x)
      } text "inputFile is a string property"
      opt[String]("outputFile") required () action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"
      opt[String]("postgressUrl") required () action { (x, c) ⇒
        c.copy(postgressUrl = x)
      } text "postgressUrl is a string property"
      opt[String]("postgressUsername") required () action { (x, c) ⇒
        c.copy(postgressUsername = x)
      } text "postgressUsername is a string property"
      opt[String]("postgressPassword") required () action { (x, c) ⇒
        c.copy(postgressPassword = x)
      } text "postgressPassword is a string property"
      opt[String]("postgressDB") required () action { (x, c) ⇒
        c.copy(postgressDB = x)
      } text "postgressDB is a string property"

      version("1.0")
      help("help") text "help text"
    }

  override def run(spark: SparkSession, config: DefaultWithDbConfig, storage: Storage): Unit = {
    import spark.implicits._

    log.info(s"Generating operator ACM csv file from [$config.inputFile] to [$config.outputFile]")

    val channelMappings = storage.channelMappings(config.postgressUrl, config.postgressDB, config.postgressUsername, config.postgressPassword)
    val operators = storage.readFromParquet[Operator](config.inputFile)
    val transformed = transform(spark, channelMappings, operators)

    val filePath = new Path(config.outputFile)
    val temporaryPath = new Path(filePath.getParent, "operator_acm_to_csv.tmp")

    storage.writeToSingleCsv(transformed, temporaryPath.toString, config.outputFile, delim = "\u00B6")
  }
}
