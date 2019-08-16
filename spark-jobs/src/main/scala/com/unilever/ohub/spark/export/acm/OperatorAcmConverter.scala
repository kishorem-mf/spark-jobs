package com.unilever.ohub.spark.export.acm

import com.unilever.ohub.spark.DomainDataProvider
import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.export.acm.model.AcmOperator
import com.unilever.ohub.spark.export._

object OperatorAcmConverter extends Converter[Operator, AcmOperator] with TypeConversionFunctions with AcmTransformationFunctions {

  override def convert(op: Operator): AcmOperator = {
    implicit val oper = op

    AcmOperator(
      OPR_ORIG_INTEGRATION_ID = getValue("ohubId"),
      OPR_LNKD_INTEGRATION_ID = getValue("concatId", Some(new OperatorOldOhubConverter(DomainDataProvider().sourceIds))),
      GOLDEN_RECORD_FLAG = getValue("isGoldenRecord", Some(BooleanToYNConverter)),
      COUNTRY_CODE = getValue("countryCode"),
      NAME = getValue("name", Some(CleanString)),
      CHANNEL = getValue("channel"),
      SUB_CHANNEL = getValue("subChannel"),
      REGION = getValue("region"),
      OTM = getValue("otm"),
      PREFERRED_PARTNER = getValue("distributorName", Some(CleanString)),
      STREET = getValue("street"),
      HOUSE_NUMBER = getValue("houseNumber"),
      ZIPCODE = getValue("zipCode"),
      CITY = getValue("city"),
      COUNTRY = getValue("countryName"),
      AVERAGE_SELLING_PRICE = getValue("averagePrice"),
      NUMBER_OF_COVERS = getValue("totalDishes"),
      NUMBER_OF_WEEKS_OPEN = getValue("weeksClosed", Some(WeeksClosedToOpened)),
      NUMBER_OF_DAYS_OPEN = getValue("daysOpen"),
      CONVENIENCE_LEVEL = getValue("cookingConvenienceLevel"),
      RESPONSIBLE_EMPLOYEE = getValue("salesRepresentative"),
      NPS_POTENTIAL = getValue("netPromoterScore"),
      CHAIN_KNOTEN = getValue("chainId"),
      CHAIN_NAME = getValue("chainName", Some(CleanString)),
      DATE_CREATED = getValue("dateCreated"),
      DATE_UPDATED = getValue("dateUpdated"),
      DELETE_FLAG = getValue("isActive", Some(InvertedBooleanToYNConverter)),
      WHOLESALER_OPERATOR_ID = getValue("distributorOperatorId"),
      PRIVATE_HOUSEHOLD = getValue("isPrivateHousehold", Some(BooleanToYNConverter)),
      VAT = getValue("vat"),
      OPEN_ON_MONDAY = getValue("isOpenOnMonday", Some(BooleanToYNConverter)),
      OPEN_ON_TUESDAY = getValue("isOpenOnTuesday", Some(BooleanToYNConverter)),
      OPEN_ON_WEDNESDAY = getValue("isOpenOnWednesday", Some(BooleanToYNConverter)),
      OPEN_ON_THURSDAY = getValue("isOpenOnThursday", Some(BooleanToYNConverter)),
      OPEN_ON_FRIDAY = getValue("isOpenOnFriday", Some(BooleanToYNConverter)),
      OPEN_ON_SATURDAY = getValue("isOpenOnSaturday", Some(BooleanToYNConverter)),
      OPEN_ON_SUNDAY = getValue("isOpenOnSunday", Some(BooleanToYNConverter)),
      KITCHEN_TYPE = getValue("kitchenType", Some(CleanString)),
      LOCAL_CHANNEL = getValue("localChannel"),
      CHANNEL_USAGE = getValue("channelUsage"),
      SOCIAL_COMMERCIAL = getValue("socialCommercial"),
      STRATEGIC_CHANNEL = getValue("strategicChannel"),
      GLOBAL_CHANNEL = getValue("globalChannel"),
      GLOBAL_SUBCHANNEL = getValue("globalSubChannel")
     // OPERATOR_ID = getValue("concatId")
    )
  }
}
