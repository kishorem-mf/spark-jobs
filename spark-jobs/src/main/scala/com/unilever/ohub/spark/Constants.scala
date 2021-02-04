package com.unilever.ohub.spark

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StringType};
object Constants {

  val CSV_PATTERN = "UFS_([A-Z0-9_-]+)_(CHAINS|LOYALTY|OPERATORS|OPERATOR_ACTIVITIES|OPERATOR_CLASSIFICATIONS|" +
    "CONTACTPERSONS|CONTACTPERSON_ACTIVITIES|CONTACTPERSON_CLASSIFICATIONS|CONTACTPERSON_SOCIAL|CONTACTPERSON_ANONYMIZATIONS|" +
    "PRODUCTS|ORDERS|ORDERLINES|ORDERS_DELETED|ORDERLINES_DELETED|SUBSCRIPTIONS|QUESTIONS|ANSWERS|CAMPAIGN_OPENS|" +
    "CAMPAIGN_CLICKS|CAMPAIGN_BOUNCES|CAMPAIGNS|CAMPAIGN_SENDS|CHANNEL_MAPPINGS|INVALID_EMAILS|INVALID_MOBILE_NUMBERS)_([0-9]{14}).csv"

  //countrycode-entity-sourceName combination
  val exclusionSourceEntityCountryList = Seq(
    Row("GB","campaignbounces","ACM")
    ,Row("GB","campaignclicks","ACM")
    ,Row("GB","campaigns","ACM")
    ,Row("GB","campaignopens","ACM")
    ,Row("GB","campaignsends","ACM")
    ,Row("GB","loyaltypoints","EMAKINA")
    ,Row("GB","orderlines","FUZZIT")
    ,Row("GB","orderlines","LOYALTY")
    ,Row("GB","orderlines","EMAKINA")
    ,Row("GB","orders","FUZZIT")
    ,Row("GB","orders","LOYALTY")
    ,Row("GB","orders","EMAKINA")
    ,Row("GB","products","FUZZIT")
    ,Row("GB","products","EMAKINA")
    ,Row("GB","products","FRONTIER")
    ,Row("GB","products","LOYALTY")
    ,Row("IE","campaignbounces","ACM")
    ,Row("IE","campaignclicks","ACM")
    ,Row("IE","campaignopens","ACM")
    ,Row("IE","campaigns","ACM")
    ,Row("IE","campaignsends","ACM")
    ,Row("IE","loyaltypoints","EMAKINA")
    ,Row("IE","orderlines","EMAKINA")
    ,Row("IE","orders","LOYALTY")
    ,Row("IE","orders","EMAKINA")
    ,Row("IE","products","EMAKINA")
    ,Row("IE","products","LOYALTY")
    ,Row("IE","questions","EMAKINA")
    ,Row("IE","orderlines","LOYALTY")
  )

//countryCode;entity;sourceName;orderType
  val includeOrderList = Seq(
    Row("GB","orders","EMAKINA","Sample")
    ,Row("GB","orderlines","EMAKINA","Sample")
    ,Row("IE","orders","EMAKINA","Sample")
    ,Row("IE","orderlines","EMAKINA","Sample")
  )

  val schemaforexclusionSourceEntityCountryList = List(StructField("countryCode",StringType),
    StructField("entity",StringType),
    StructField("sourceName",StringType))

  val schemaforincludeOrderList = List(StructField("countryCode",StringType),
    StructField("entity",StringType),
    StructField("sourceName",StringType),
    StructField("orderType",StringType))

}
