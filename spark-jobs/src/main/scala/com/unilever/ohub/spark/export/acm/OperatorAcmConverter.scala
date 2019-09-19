package com.unilever.ohub.spark.export.acm

import com.unilever.ohub.spark.DomainDataProvider
import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.export._
import com.unilever.ohub.spark.export.acm.model.AcmOperator

object OperatorAcmConverter extends Converter[Operator, AcmOperator] with TypeConversionFunctions with AcmTypeConversionFunctions {

  override def convert(implicit op: Operator, explain: Boolean = false): AcmOperator = {
    AcmOperator(
      OPR_ORIG_INTEGRATION_ID = getValue("ohubId"),
      OPR_LNKD_INTEGRATION_ID = getValue("concatId", new OperatorOldOhubConverter(DomainDataProvider().sourceIds)),
      GOLDEN_RECORD_FLAG = getValue("isGoldenRecord", BooleanToYNConverter),
      COUNTRY_CODE = getValue("countryCode"),
      NAME = getValue("name", CleanString),
      CHANNEL = getValue("channel"),
      SUB_CHANNEL = getValue("subChannel"),
      REGION = getValue("region"),
      OTM = getValue("otm"),
      PREFERRED_PARTNER = getValue("distributorName", CleanString),
      STREET = getValue("street"),
      HOUSE_NUMBER = getValue("houseNumber"),
      ZIPCODE = getValue("zipCode"),
      CITY = getValue("city"),
      COUNTRY = getValue("countryName"),
      AVERAGE_SELLING_PRICE = getValue("averagePrice"),
      NUMBER_OF_COVERS = getValue("totalDishes"),
      NUMBER_OF_WEEKS_OPEN = getValue("weeksClosed", WeeksClosedToOpened),
      NUMBER_OF_DAYS_OPEN = getValue("daysOpen"),
      CONVENIENCE_LEVEL = getValue("cookingConvenienceLevel"),
      RESPONSIBLE_EMPLOYEE = getValue("salesRepresentative"),
      NPS_POTENTIAL = getValue("netPromoterScore"),
      CHAIN_KNOTEN = getValue("chainId"),
      CHAIN_NAME = getValue("chainName", CleanString),
      DATE_CREATED = getValue("dateCreated"),
      DATE_UPDATED = getValue("dateUpdated"),
      DELETE_FLAG = getValue("isActive", InvertedBooleanToYNConverter),
      WHOLESALER_OPERATOR_ID = getValue("distributorOperatorId"),
      PRIVATE_HOUSEHOLD = getValue("isPrivateHousehold", BooleanToYNConverter),
      VAT = getValue("vat"),
      OPEN_ON_MONDAY = getValue("isOpenOnMonday", BooleanToYNConverter),
      OPEN_ON_TUESDAY = getValue("isOpenOnTuesday", BooleanToYNConverter),
      OPEN_ON_WEDNESDAY = getValue("isOpenOnWednesday", BooleanToYNConverter),
      OPEN_ON_THURSDAY = getValue("isOpenOnThursday", BooleanToYNConverter),
      OPEN_ON_FRIDAY = getValue("isOpenOnFriday", BooleanToYNConverter),
      OPEN_ON_SATURDAY = getValue("isOpenOnSaturday", BooleanToYNConverter),
      OPEN_ON_SUNDAY = getValue("isOpenOnSunday", BooleanToYNConverter),
      KITCHEN_TYPE = getValue("kitchenType", CleanString),
      LOCAL_CHANNEL = getValue("localChannel"),
      CHANNEL_USAGE = getValue("channelUsage"),
      SOCIAL_COMMERCIAL = getValue("socialCommercial"),
      STRATEGIC_CHANNEL = getValue("strategicChannel"),
      GLOBAL_CHANNEL = getValue("globalChannel"),
      GLOBAL_SUBCHANNEL = getValue("globalSubChannel"),
      TARGET_OHUB_ID = getValue("additionalFields", new GetAdditionalField("targetOhubId"))
     // OPERATOR_ID = getValue("concatId")
    )
  }
}
