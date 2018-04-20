package com.unilever.ohub.spark.tsv2parquet.fuzzit

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.generic.StringFunctions._
import com.unilever.ohub.spark.tsv2parquet.CustomParsers._
import com.unilever.ohub.spark.tsv2parquet.DomainTransformer
import org.apache.spark.sql.Row

object OperatorConverter extends FuzzitDomainGateKeeper[Operator] {

  val CUSTOMER_UUID = "CUSTOMER_UUID"
  val SALES_ORG = "SALES_ORG"
  val ACCOUNT_MRDR = "ACCOUNT_MRDR"
  val ORIGIN = "ORIGIN"
  val WS_MRDR = "WS_MRDR"
  val WHOLESALER_ID = "WHOLESALER_ID"
  val NAME_1 = "NAME_1"
  val NAME_2 = "NAME_2"
  val STREET = "STREET"
  val ZIP = "ZIP"
  val CITY = "CITY"
  val SALES_REP = "SALES_REP"
  val SALES_REP_NAME = "SALES_REP_NAME"
  val EMAIL = "EMAIL"
  val FAX = "FAX"
  val PHONE_1 = "PHONE_1"
  val PHONE_2 = "PHONE_2"
  val EXT_CUST_HIER_1 = "EXT_CUST_HIER_1"
  val EXT_CUST_HIER_1_TEXT = "EXT_CUST_HIER_1_TEXT"
  val EXT_CUST_HIER_2 = "EXT_CUST_HIER_2"
  val EXT_CUST_HIER_2_TEXT = "EXT_CUST_HIER_2_TEXT"
  val CAM_KEY = "CAM_KEY"
  val CAM_TEXT = "CAM_TEXT"
  val CHAIN_KNOTEN = "CHAIN_KNOTEN"
  val CHAIN_NAME = "CHAIN_NAME"
  val CHANNEL_KEY = "CHANNEL_KEY"
  val CHANNEL_TEXT = "CHANNEL_TEXT"
  val CPU_KEY = "CPU_KEY"
  val CPU_TEXT = "CPU_TEXT"
  val AUSSCHREIBUNG = "AUSSCHREIBUNG"
  val SEGMENT_KEY = "SEGMENT_KEY"
  val SEGMENT_TEXT = "SEGMENT_TEXT"
  val STUETZUNG = "STUETZUNG"
  val CUST_SUB_SEG_EXT = "CUST_SUB_SEG_EXT"
  val CUST_SET_EXT = "CUST_SET_EXT"
  val CUST_SEG_KEY_EXT = "CUST_SEG_KEY_EXT"
  val GLN = "GLN"
  val TAX_CODE = "TAX_CODE"
  val IGNORE = "IGNORE"
  val CREATION_DATE = "CREATION_DATE"
  val CUST_GRP_EXT = "CUST_GRP_EXT"

  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ Operator = { transformer ⇒ row ⇒
    import transformer._
    implicit val source: Row = row

    useHeaders(fuzzitHeaders)

    // format: OFF

    val sourceName                                    =   "FUZZIT"
    val salesOrg                                      =   mandatoryValue(SALES_ORG, "countryCode")(row)
    val countryCode                                   =   countryCodeBySalesOrg(salesOrg).get // TODO improve error message, don't do a .get here
    val sourceEntityId                                =   mandatoryValue(CUSTOMER_UUID, "concatId")(row)
    val concatNames                                   =   concatValues(NAME_1, NAME_2)
    val concatId                                      =   DomainEntity.createConcatIdFromValues(countryCode, sourceName, sourceEntityId)
    val ohubCreated                                   =   currentTimestamp()
    val (street, houseNumber, houseNumberExtension)   =   splitAddress(STREET, "street")

    // set additional fields first

    additionalField(CAM_KEY, "germanChainId")
    additionalField(CAM_TEXT, "germanChainName")

    Operator(
      // fieldName                  mandatory   sourceFieldName           targetFieldName                 transformationFunction (unsafe)
      concatId                    = concatId                                                                                                           ,
      countryCode                 = countryCode                                                                                                        ,
      customerType                = Operator.customerType                                                                                              ,
      dateCreated                 = optional ( CREATION_DATE,            "dateCreated",                  parseDateTimeForPattern()                    ),
      dateUpdated                 = optional ( CREATION_DATE,            "dateUpdated",                  parseDateTimeForPattern()                    ),
      isActive                    = true                                                                                                               ,
      isGoldenRecord              = false                                                                                                              ,
      ohubId                      = None                                                                                                               ,
      name                        = concatNames                                                                                                        ,
      sourceEntityId              = mandatory ( CUSTOMER_UUID,            "sourceEntityId"                                                            ),
      sourceName                  = sourceName                                                                                                         ,
      ohubCreated                 = ohubCreated                                                                                                        ,
      ohubUpdated                 = ohubCreated                                                                                                        ,
      averagePrice                = None                                                                                                               ,
      chainId                     = optional  ( CHAIN_KNOTEN,             "chainId"                                                                   ),
      chainName                   = optional  ( CHAIN_NAME,               "chainName"                                                                 ),
      channel                     = optional  ( CHANNEL_TEXT,             "channel"                                                                   ),
      city                        = optional  ( CITY,                     "city"                                                                      ),
      cookingConvenienceLevel     = None                                                                                                               ,
      countryName                 = countryName(countryCode)                                                                                           ,
      daysOpen                    = None                                                                                                               ,
      distributorName             = None                                                                                                               ,
      distributorOperatorId       = None                                                                                                               ,
      emailAddress                = optional  ( EMAIL,                    "emailAddress"                                                              ),
      faxNumber                   = optional  ( FAX,                      "faxNumber",                    cleanPhone(countryCode)                     ),
      hasDirectMailOptIn          = None                                                                                                               ,
      hasDirectMailOptOut         = None                                                                                                               ,
      hasEmailOptIn               = None                                                                                                               ,
      hasEmailOptOut              = None                                                                                                               ,
      hasFaxOptIn                 = None                                                                                                               ,
      hasFaxOptOut                = None                                                                                                               ,
      hasGeneralOptOut            = None                                                                                                               ,
      hasMobileOptIn              = None                                                                                                               ,
      hasMobileOptOut             = None                                                                                                               ,
      hasTelemarketingOptIn       = None                                                                                                               ,
      hasTelemarketingOptOut      = None                                                                                                               ,
      houseNumber                 = houseNumber                                                                                                        ,
      houseNumberExtension        = houseNumberExtension                                                                                               ,
      isNotRecalculatingOtm       = None                                                                                                               ,
      isOpenOnFriday              = None                                                                                                               ,
      isOpenOnMonday              = None                                                                                                               ,
      isOpenOnSaturday            = None                                                                                                               ,
      isOpenOnSunday              = None                                                                                                               ,
      isOpenOnThursday            = None                                                                                                               ,
      isOpenOnTuesday             = None                                                                                                               ,
      isOpenOnWednesday           = None                                                                                                               ,
      isPrivateHousehold          = None                                                                                                               ,
      kitchenType                 = None                                                                                                               ,
      mobileNumber                = None                                                                                                               ,
      netPromoterScore            = None                                                                                                               ,
      oldIntegrationId            = None                                                                                                               ,
      otm                         = None                                                                                                               ,
      otmEnteredBy                = None                                                                                                               ,
      phoneNumber                 = optional  ( PHONE_1,                  "phoneNumber",                  cleanPhone(countryCode)                     ),
      region                      = None                                                                                                               ,
      salesRepresentative         = optional  ( SALES_REP,                "salesRepresentative"                                                       ),
      state                       = None                                                                                                               ,
      street                      = street                                                                                                             ,
      subChannel                  = None                                                                                                               ,
      totalDishes                 = None                                                                                                               ,
      totalLocations              = None                                                                                                               ,
      totalStaff                  = None                                                                                                               ,
      vat                         = None                                                                                                               ,
      webUpdaterId                = None                                                                                                               ,
      weeksClosed                 = None                                                                                                               ,
      zipCode                     = optional  ( ZIP,                      "zipCode"                                                                   ),
      additionalFields            = additionalFields                                                                                                   ,
      ingestionErrors             = errors
    )
    // format: ON
  }

  private lazy val fuzzitHeaders: Map[String, Int] =
    Seq(
      CUSTOMER_UUID,
      SALES_ORG,
      ACCOUNT_MRDR,
      ORIGIN,
      WS_MRDR,
      WHOLESALER_ID,
      NAME_1,
      NAME_2,
      STREET,
      ZIP,
      CITY,
      SALES_REP,
      SALES_REP_NAME,
      EMAIL,
      FAX,
      PHONE_1,
      PHONE_2,
      EXT_CUST_HIER_1,
      EXT_CUST_HIER_1_TEXT,
      EXT_CUST_HIER_2,
      EXT_CUST_HIER_2_TEXT,
      CAM_KEY,
      CAM_TEXT,
      CHAIN_KNOTEN,
      CHAIN_NAME,
      CHANNEL_KEY,
      CHANNEL_TEXT,
      CPU_KEY,
      CPU_TEXT,
      AUSSCHREIBUNG,
      SEGMENT_KEY,
      SEGMENT_TEXT,
      STUETZUNG,
      CUST_SUB_SEG_EXT,
      CUST_SET_EXT,
      CUST_SEG_KEY_EXT,
      GLN,
      TAX_CODE,
      IGNORE,
      CREATION_DATE,
      CUST_GRP_EXT
    ).zipWithIndex.toMap
}
