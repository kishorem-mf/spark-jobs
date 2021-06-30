package com.unilever.ohub.spark.merging

import java.text.SimpleDateFormat
import java.util.UUID
import java.util.UUID._
import java.time.Instant

import com.unilever.ohub.spark.domain.entity.{WholesalerAssignment, _}
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{SparkJob, SparkJobConfig}
import org.apache.spark
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import scopt.OptionParser

case class WholesalerAssignmentMergingConfig(
                                       wholesalerAssignmentInputFile: String = "wholesalerassignment-input-file",
                                       previousIntegrated: String = "previous-integrated-file",
                                       operatorIntegrated: String = "operator-input-file",
                                       operatorGoldenIntegrated: String = "operator-golden-input-file",
                                       outputFile: String = "path-to-output-file",
                                       orderIntegrated: String = "order-input-file",
                                       orderlineIntegrated: String = "orderline-input-file",
                                       productIntegrated: String = "product-input-file",
                                       executionDate: String = "execution-date"
                                     ) extends SparkJobConfig

object WholesalerAssignmentMerging extends SparkJob[WholesalerAssignmentMergingConfig] {

  case class IngestionError(originalColumnName: String, inputValue: Option[String], exceptionMessage: String)

  def transform(
                 spark: SparkSession,
                 wholesalerAssignmentInputFile: Dataset[WholesalerAssignment],
                 previousIntegrated: Dataset[WholesalerAssignment],
                 operator: Dataset[Operator]
               ): Dataset[WholesalerAssignment] = {
    import spark.implicits._

    previousIntegrated
      .joinWith(wholesalerAssignmentInputFile, previousIntegrated("concatId") === wholesalerAssignmentInputFile("concatId"), JoinType.FullOuter)
      .map {
        case (integrated, ws) ⇒
          if (ws == null) {
            integrated
          } else {
            val ohubId = if (integrated == null) Some(UUID.randomUUID().toString) else integrated.ohubId
            ws.copy(ohubId = ohubId, isGoldenRecord = true)
          }
      }  // update opr ids
      .joinWith(operator, $"operatorConcatId" === operator("concatId"), "left")
      .map {
        case (ws: WholesalerAssignment, opr: Operator) => ws.copy(operatorOhubId = opr.ohubId)
        case (ws, _) => ws
      }
  }

  private def renameColumnsDataFrame(
                              dataFrame: DataFrame,
                              prefix: String = "",
                              postfix: String = ""
                            ): DataFrame = {
    val newFields = dataFrame.columns
      .map(value => s"${prefix}${value.trim.capitalize}${postfix}")
      .toSeq
    dataFrame.toDF(newFields: _*)
  }

  private def ordersPreparation1(
                          orders: Dataset[Order]
                        ): DataFrame = {
    orders
      .where(col("operatorConcatId").isNotNull)
      .where(col("type").isin("SSD"))
      .where(col("distributorId").isNotNull)
      .where(
        (upper(col("sourceName"))
          .like("%CONCESSIONAIRES_AT%")
          .and(col("distributorOperatorId").like("_"))
        )
          .or(not(upper(col("sourceName")).like("%CONCESSIONAIRES_AT%")))
      )
      .where(col("isActive").===(true))
      .select(
        col("operatorConcatId"),
        col("distributorId"),
        col("concatId"),
        col("transactionDate")
      )
      .distinct
      .orderBy(col("operatorConcatId"), col("distributorId"))
  }

  private def orderlinesPreparation1(
                            orderlines: Dataset[OrderLine],
                            products: Dataset[Product]
                          ): DataFrame = {
    orderlines
      .where(col("isActive").===(true))
      .join(
        products.withColumnRenamed("sourceName","productSourceName"),
        orderlines("productConcatId").===(products("concatId")),
        "left"
      )
      .select(
        col("orderConcatId"),
        col("sourceName"),
        col("categoryLevel3"),
        col("amount"),
        col("distributionChannel"),
        col("division")
      )
      .distinct
  }

  private def ordersPreparation2(
                          orderlines1: DataFrame,
                          orders1: DataFrame,
                          today: String
                          ): DataFrame = {
    val deFoodDistributionChannel = "22"
    val deFoodDivision = "50"
    val deIcecreamDistributionChannel = "20"
    val deIcecreamDivision = "11"
    orderlines1
        .join(orders1, col("orderConcatId").===(col("concatId")), "inner")
        .drop("concatId")
        .orderBy("orderConcatId")
        .withColumn(
          "foodAmount",
          when(
            (col("distributionChannel")
              .isin(deFoodDistributionChannel)
              .and(col("division").isin(deFoodDivision))
              .and(
                col("transactionDate").>=(date_format(lit(today), "YYYY-MM-DD"))
              ))
              .or(
                col("sourceName")
                  .===("FUZZIT")
                  .and(
                    col("transactionDate").>=(date_format(lit(today), "YYYY-MM-DD"))
                  )
              ),
            col("amount")
          )
        )
        .withColumn(
          "icecreamAmount",
          when(
            col("distributionChannel")
              .isin(deIcecreamDistributionChannel)
              .and(col("division").isin(deIcecreamDivision))
              .and(
                col("transactionDate").>=(date_format(lit(today), "YYYY-MM-DD"))
              ),
            col("amount")
          )
        )
  }

  private def assignmentsPreparation1(
                          operators: Dataset[Operator],
                          orders3: DataFrame
                        ): DataFrame = {
    operators
      .select(
        col("countryCode").as("oprCountryCode"),
        col("ohubId"),
        col("concatId")
      )
      .distinct
      .join(orders3, col("concatId").===(col("operatorConcatId")), "inner")
      .drop("concatId")
      .withColumnRenamed("ohubId", "operatorOhubId")
  }

  private def assignmentsPreparation2(
                          operators: Dataset[Operator],
                          assignments1: DataFrame,
                          sapSource: String
                        ): DataFrame = {
    operators
      .where(col("sourceName").isin(sapSource))
      .select(
        col("countryCode"),
        col("ohubId"),
        split(col("sourceEntityId"), "-")(0).as("sourceEntityId")
      )
      .distinct
      .join(
        assignments1,
        (col("distributorId")
          .===(col("sourceEntityId")))
          .and(col("oprCountryCode").===(col("countryCode"))),
        "inner"
      )
      .drop(
        "oprCountryCode",
        "countryCode",
        "sourceEntityId",
        "operatorConcatId",
        "distributorId"
      )
      .withColumnRenamed("ohubId", "distributorOhubId")
  }

  private def assignmentsPreparation3(
                          assignments2: DataFrame
                        ): DataFrame = {
    assignments2
      .groupBy(col("operatorOhubId"), col("distributorOhubId"))
      .agg(
        sum(col("foodAmount")).as("foodAmount"),
        sum(col("icecreamAmount")).as("icecreamAmount")
      )
      .orderBy(col("operatorOhubId"))
  }

  private def assignmentsPreparation5(
                            operators_golden: Dataset[OperatorGolden],
                            assignments4: DataFrame
                          ): DataFrame = {
    val uuid =udf(() => s"rvm-${java.util.UUID.randomUUID().toString}")
    val unixTime = udf(() => Instant.now.getEpochSecond.toString)
    assignments4
      .join(operators_golden.select(col("ohubId"), col("crmId"), col("countryCode")),
        col("distributorOhubId").===(col("ohubId")),"inner")
      .withColumn("id", uuid())
      .withColumn("creationTimestamp", current_timestamp())
      .withColumn("concatId",concat_ws("~",col("countryCode"),lit("SSD"),col("operatorOhubId"),col("distributorOhubId")))
      .withColumn("countryCode", col("countryCode"))
      .withColumn("customerType", lit("WHOLESALERASSIGNMENT"))
      .withColumn("dateCreated", current_timestamp)
      .withColumn("dateUpdated", current_timestamp)
      .withColumn("isActive", lit(true))
      .withColumn("isGoldenRecord", lit(true))
      .withColumn("ohubId", uuid())
      .withColumn("operatorOhubId", col("operatorOhubId"))
      .withColumn("operatorConcatId", col("operatorCrmId"))
      .withColumn("sourceEntityId",concat_ws("~", col("operatorOhubId"), col("distributorOhubId")))
      .withColumn("sourceName", lit("SSD"))
      .withColumn("ohubCreated", current_timestamp)
      .withColumn("ohubUpdated", current_timestamp)
      .withColumn("isPrimaryFoodsWholesaler", col("isPrimaryFoodsWholesaler"))
      .withColumn("isPrimaryIceCreamWholesaler", col("isPrimaryIcecreamWholesaler"))
      .withColumn("isPrimaryFoodsWholesalerCrm", lit("").cast(BooleanType))
      .withColumn("isPrimaryIceCreamWholesalerCrm", lit("").cast(BooleanType))
      .withColumn("wholesalerCustomerCode2", lit("").cast(StringType))
      .withColumn("wholesalerCustomerCode3", lit("").cast(StringType))
      .withColumn("hasPermittedToShareSsd", lit("").cast(BooleanType))
      .withColumn("isProvidedByCrm", lit("").cast(BooleanType))
      .withColumn("crmId", col("crmId"))
      .withColumn("additionalFields", typedLit(Map.empty[String, String]))
      .withColumn("ingestionErrors", typedLit(Map.empty[String, IngestionError]))
      .select(
        col("id"),col("creationTimestamp"),col("concatId"),col("countryCode"),col("customerType"),col("dateCreated"),
        col("dateUpdated"),col("isActive"),col("isGoldenRecord"),col("ohubId"),
        col("operatorOhubId"),col("operatorConcatId"),col("sourceEntityId"),col("sourceName"),col("ohubCreated"),
        col("ohubUpdated"),col("isPrimaryFoodsWholesaler"),col("isPrimaryIceCreamWholesaler"),
        col("isPrimaryFoodsWholesalerCrm"),col("isPrimaryIceCreamWholesalerCrm"),
        col("wholesalerCustomerCode2"),col("wholesalerCustomerCode3"),col("hasPermittedToShareSsd"),col("isProvidedByCrm"),
        col("crmId"),col("additionalFields"),col("ingestionErrors")
      )
  }

  private def mergedWholesalerAssignmentsWithRtmIcCategoryProcess(
                                                  mergedWholesalerAssignments: DataFrame,
                                                  operators_golden: Dataset[OperatorGolden]
                                                ): DataFrame = {
    mergedWholesalerAssignments
      .join(
        operators_golden.select(
          col("ohubId").as("oprOhubId"),
          col("routeToMarketIceCreamCategory"),
          col("sourceName").as("oprSourceName")
        ),
        col("oprOhubId").===(col("operatorOhubId")),
        "left"
      )
      .drop(col("oprOhubId"))
      .withColumn(
        "routeToMarketIceCreamCategory",
        when(upper(col("sourceName")).like("%CONCESSIONAIRES_AT%"), lit("VPK"))
          .otherwise(
            when(upper(col("sourceName")).like("%CONCESSIONAIRES_CH%"), lit("VPK"))
              .otherwise(col("routeToMarketIceCreamCategory"))
          )
      )
      .withColumn(
        "isPrimaryFoodsWholesaler",
        when(upper(col("sourceName")).like("%CONCESSIONAIRES_AT%"), lit(false))
          .otherwise(
            when(upper(col("sourceName")).like("%CONCESSIONAIRES_CH%"), lit(false))
              .otherwise(col("isPrimaryFoodsWholesaler"))
          )
      )
      .select(
              "id","creationTimestamp","concatId","countryCode","customerType","dateCreated",
              "dateUpdated","isActive","isGoldenRecord","ohubId","operatorOhubId","operatorConcatId",
              "sourceEntityId","sourceName","ohubCreated","ohubUpdated","routeToMarketIceCreamCategory",
              "isPrimaryFoodsWholesaler","isPrimaryIceCreamWholesaler","isPrimaryFoodsWholesalerCrm",
              "isPrimaryIceCreamWholesalerCrm","wholesalerCustomerCode2","wholesalerCustomerCode3",
              "hasPermittedToShareSsd","isProvidedByCrm","crmId","additionalFields","ingestionErrors"
      )
  }

  private def mergedUDLrecord(assignments5: DataFrame, emptyWholesalerAssignmentsRenamed: DataFrame): DataFrame = {
    assignments5.join(emptyWholesalerAssignmentsRenamed,col("sourceEntityId").===(col("rawSourceEntityId")),"outer")
      .withColumn("id",when(col("rawId").isNull,col("id")).otherwise(col("rawId")))
      .withColumn("creationTimestamp",when(col("rawCreationTimestamp").isNull,col("creationTimestamp")).otherwise(col("rawCreationTimestamp")))
      .withColumn("concatId",when(col("rawConcatId").isNull,col("concatId")).otherwise(col("rawConcatId")))
      .withColumn("countryCode",when(col("rawCountryCode").isNull,col("countryCode")).otherwise(col("rawCountryCode")))
      .withColumn("customerType",when(col("rawCustomerType").isNull,col("customerType")).otherwise(col("rawCustomerType")))
      .withColumn("dateCreated",when(col("rawDateCreated").isNull,col("dateCreated")).otherwise(col("rawDateCreated")))
      .withColumn("dateUpdated",when(col("rawDateUpdated").isNull,col("dateUpdated")).otherwise(col("rawDateUpdated")))
      .withColumn("isActive",when(col("rawIsActive").isNull,col("isActive")).otherwise(col("rawIsActive")))
      .withColumn("isGoldenRecord",when(col("rawIsGoldenRecord").isNull,col("isGoldenRecord")).otherwise(col("rawIsGoldenRecord")))
      .withColumn("ohubId",when(col("rawOhubId").isNull,col("ohubId")).otherwise(col("rawOhubId")))
      .withColumn("operatorOhubId",when(col("rawOperatorOhubId").isNull,col("operatorOhubId")).otherwise(col("rawOperatorOhubId")))
      .withColumn("operatorConcatId",when(col("rawOperatorConcatId").isNull,col("operatorConcatId")).otherwise(col("rawOperatorConcatId")))
      .withColumn("sourceEntityId",when(col("rawSourceEntityId").isNull,col("sourceEntityId")).otherwise(col("rawSourceEntityId")))
      .withColumn("sourceName",when(col("rawSourceName").isNull,col("sourceName")).otherwise(col("rawSourceName")))
      .withColumn("ohubCreated",when(col("rawOhubCreated").isNull,col("ohubCreated")).otherwise(col("rawOhubCreated")))
      .withColumn("ohubUpdated",when(col("rawOhubUpdated").isNull,col("ohubUpdated")).otherwise(col("rawOhubUpdated")))
      .withColumn("isPrimaryFoodsWholesaler",col("isPrimaryFoodsWholesaler"))
      .withColumn("isPrimaryIceCreamWholesaler",col("isPrimaryIceCreamWholesaler"))
      .withColumn("isPrimaryFoodsWholesalerCrm",when(col("rawIsPrimaryFoodsWholesalerCrm").isNull,col("isPrimaryFoodsWholesalerCrm")).otherwise(col("rawIsPrimaryFoodsWholesalerCrm")))
      .withColumn("isPrimaryIceCreamWholesalerCrm",when(col("rawIsPrimaryIceCreamWholesalerCrm").isNull,col("isPrimaryIceCreamWholesalerCrm")).otherwise(col("rawIsPrimaryIceCreamWholesalerCrm")))
      .withColumn("wholesalerCustomerCode2",when(col("rawWholesalerCustomerCode2").isNull,col("wholesalerCustomerCode2")).otherwise(col("rawWholesalerCustomerCode2")))
      .withColumn("wholesalerCustomerCode3",when(col("rawWholesalerCustomerCode3").isNull,col("wholesalerCustomerCode3")).otherwise(col("rawWholesalerCustomerCode3")))
      .withColumn("hasPermittedToShareSsd",when(col("rawHasPermittedToShareSsd").isNull,col("hasPermittedToShareSsd")).otherwise(col("rawHasPermittedToShareSsd")))
      .withColumn("isProvidedByCrm",when(col("rawIsProvidedByCrm").isNull,col("isProvidedByCrm")).otherwise(col("rawIsProvidedByCrm")))
      .withColumn("crmId",when(col("rawCrmId").isNull,col("crmId")).otherwise(col("rawCrmId")))
      .withColumn("additionalFields",when(col("rawAdditionalFields").isNull,col("additionalFields")).otherwise(col("rawAdditionalFields")))
      .withColumn("ingestionErrors",when(col("rawIngestionErrors").isNull,col("ingestionErrors")).otherwise(col("rawIngestionErrors")))
      .select(
        col("id"),col("creationTimestamp"),col("concatId"),col("countryCode"),
        col("customerType"),col("dateCreated"),col("dateUpdated"),
        col("isActive"),col("isGoldenRecord"),col("ohubId"),
        col("operatorOhubId"),col("operatorConcatId"),col("sourceEntityId"),
        col("sourceName"),col("ohubCreated"),col("ohubUpdated"),
        col("isPrimaryFoodsWholesaler"),col("isPrimaryIceCreamWholesaler"),
        col("isPrimaryFoodsWholesalerCrm"),col("isPrimaryIceCreamWholesalerCrm"),
        col("wholesalerCustomerCode2"),col("wholesalerCustomerCode3"),
        col("hasPermittedToShareSsd"),col("isProvidedByCrm"),col("crmId"),
        col("additionalFields"),col("ingestionErrors")
      )
      .orderBy(col("operatorOhubId"))
  }

  private def getEmptyWholesalerAssignment(spark: SparkSession):DataFrame = {
    import com.unilever.ohub.spark.domain.entity.WholesalerAssignment
    import spark.implicits._
    val encoder = Encoders.product[WholesalerAssignment]
    spark.emptyDataset[WholesalerAssignment].toDF
  }

  private def transformToDataSet(spark: SparkSession, df: DataFrame): Dataset[WholesalerAssignment] = {
    import com.unilever.ohub.spark.domain.entity.WholesalerAssignment
    import spark.implicits._
    val encoder = Encoders.product[WholesalerAssignment]
    df.as[WholesalerAssignment]
  }

  private def extractAssignments(
                 spark: SparkSession,
                 operators: Dataset[Operator],
                 operators_golden: Dataset[OperatorGolden],
                 previousIntegrated: Dataset[WholesalerAssignment],
                 orders: Dataset[Order],
                 orderlines: Dataset[OrderLine],
                 products: Dataset[Product],
                 executionDate: String
  ): Dataset[WholesalerAssignment] = {
    import spark.implicits._
    val sapSource = "SAP_SIRIUS"
    val crmSource = List("FRONTIER", "CRM")
    val countryCodes = List("DE","CH","AT","GB","IE")
    val today = executionDate
    // DBTITLE 1,Empty wholesaler dataframe
    val emptyWholesalerAssignments = getEmptyWholesalerAssignment(spark)
    val emptyWholesalerAssignmentsRenamed = renameColumnsDataFrame(emptyWholesalerAssignments, "raw")
    val orders1 = ordersPreparation1(orders)
    val orderlines1 = orderlinesPreparation1(orderlines, products)
    val orders2 = ordersPreparation2(orderlines1, orders1, executionDate)
    val orders3 = orders2
      .groupBy(col("operatorConcatId"), col("distributorId"))
      .agg(sum(col("foodAmount")).as("foodAmount"),sum(col("icecreamAmount")).as("icecreamAmount"))
    val assignments1 = assignmentsPreparation1(operators, orders3)
    val assignments2 = assignmentsPreparation2(operators, assignments1, sapSource)
    val assignments3 = assignmentsPreparation3(assignments2)
    val foodAmountWindow = Window.partitionBy(col("operatorOhubId")).orderBy(desc("foodAmount"))
    val icecreamAmountWindow = Window.partitionBy(col("operatorOhubId")).orderBy(desc("icecreamAmount"))
    val assignments4 = assignments3.join(operators_golden, col("ohubId").===(col("operatorOhubId")), "inner")
      .withColumn("isPrimaryFoodsWholesaler",when(row_number.over(foodAmountWindow).===(1).and(col("foodAmount").>(0)), true).otherwise(false))
      .withColumn("isPrimaryIcecreamWholesaler",
        when(
          col("countryCode").isin("DE"),
          when(upper(col("accountSubType")).===("DIRECT (DELIVERED BY UL)"), false)
            .otherwise(when(upper(col("accountSubType")).===("DIRECT (CENTRALLY BILLED)"),true
              ).otherwise(when(row_number.over(icecreamAmountWindow).===(1).and(col("icecreamAmount").>(0)), true).otherwise(false))))
          .otherwise(when(row_number.over(icecreamAmountWindow).===(1).and(col("icecreamAmount").>(0)), true).otherwise(false))
      )
      .select(col("operatorOhubId"), col("distributorOhubId"),col("accountSubType"), col("accountType"),
        col("isIndirectAccount"), col("crmId").as("operatorCrmId"),col("sapCustomerId"), col("foodAmount"),
        col("icecreamAmount"), col("isPrimaryFoodsWholesaler"),col("isPrimaryIcecreamWholesaler"))
    val assignments5 = assignmentsPreparation5(operators_golden, assignments4)
    val mergedWholesalerAssignments = mergedUDLrecord(assignments5, emptyWholesalerAssignmentsRenamed)
    val mergedWholesalerAssignmentsWithRtmIcCategory =  mergedWholesalerAssignmentsWithRtmIcCategoryProcess(mergedWholesalerAssignments, operators_golden)
    transformToDataSet(spark, mergedWholesalerAssignmentsWithRtmIcCategory)
  }

  override private[spark] def defaultConfig = WholesalerAssignmentMergingConfig()

  override private[spark] def configParser(): OptionParser[WholesalerAssignmentMergingConfig] =
    new scopt.OptionParser[WholesalerAssignmentMergingConfig]("AssetMovement merging") {
      head("merges wholesaler assignment into an integrated wholesaler assignment output file.", "1.0")
      opt[String]("wholesalerAssignmentInputFile") required () action { (x, c) ⇒
        c.copy(wholesalerAssignmentInputFile = x)
      } text "WholeSalerAssignmentInputFile is a string property"
      opt[String]("previousIntegrated") required () action { (x, c) ⇒
        c.copy(previousIntegrated = x)
      } text "previousIntegrated is a string property"
      opt[String]("operatorIntegrated") required () action { (x, c) ⇒
        c.copy(operatorIntegrated = x)
      } text "operatorIntegrated is a string property"
      opt[String]("outputFile") required () action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"
      opt[String]("operatorGoldenIntegrated") required () action { (x, c) ⇒
        c.copy(operatorGoldenIntegrated = x)
      } text "operatorGoldenIntegrated is a string property"
      opt[String]("orderIntegrated") required () action { (x, c) ⇒
        c.copy(orderIntegrated = x)
      } text "orderIntegrated is a string property"
      opt[String]("orderlineIntegrated") required () action { (x, c) ⇒
        c.copy(orderlineIntegrated = x)
      } text "orderlineIntegrated is a string property"
      opt[String]("productIntegrated") required () action { (x, c) ⇒
        c.copy(productIntegrated = x)
      } text "productIntegrated is a string property"
      opt[String]("executionDate") required () action { (x, c) ⇒
        c.copy(executionDate = x)
      } text "executionDate is a string property"
      version("1.0")
      help("help") text "help text"
    }

  override def run(spark: SparkSession, config: WholesalerAssignmentMergingConfig, storage: Storage): Unit = {
    log.info(
      s"Merging AssetMovements from [${config.wholesalerAssignmentInputFile}] and  [${config.previousIntegrated}] to [${config.outputFile}]"
    )
    val wholeSalerAssignments = storage.readFromParquet[WholesalerAssignment](config.wholesalerAssignmentInputFile)
    val previousIntegrated = storage.readFromParquet[WholesalerAssignment](config.previousIntegrated)
    val orderIntegrated = storage.readFromParquet[Order](config.orderIntegrated)
    val orderlineIntegrated = storage.readFromParquet[OrderLine](config.orderlineIntegrated)
    val productIntegrated = storage.readFromParquet[Product](config.productIntegrated)
    val operatorIntegrated = storage.readFromParquet[Operator](config.operatorIntegrated)
    val operatorGoldenIntegrated = storage.readFromParquet[OperatorGolden](config.operatorGoldenIntegrated)
    val transformed = transform(spark, wholeSalerAssignments, previousIntegrated,operatorIntegrated)
    val assignments = extractAssignments(
      spark, operatorIntegrated, operatorGoldenIntegrated,
      previousIntegrated, orderIntegrated, orderlineIntegrated,
      productIntegrated, config.executionDate)
    val merged = assignments.unionByName(transformed)
    storage.writeToParquet(merged, config.outputFile)
  }
}

