package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.export._
import com.unilever.ohub.spark.export.dispatch.model.DispatchOperator

private[export] object BooleanToADConverter extends TransformationFunction[Boolean] {
  def impl(input: Boolean) = if (input) "A" else "D"

  val description: String = "Converts a boolean: true -> A(ctive) or  false -> D(eactivated)"
}

object OperatorDispatchConverter extends Converter[Operator, DispatchOperator] with TypeConversionFunctions with DispatchTransformationFunctions {

  override def convert(implicit operator: Operator, explain: Boolean = false): DispatchOperator = {
    DispatchOperator(
      COUNTRY_CODE = getValue("countryCode"),
      OPR_ORIG_INTEGRATION_ID = getValue("concatId"),
      OPR_LNKD_INTEGRATION_ID = getValue("ohubId"),
      GOLDEN_RECORD_FLAG = getValue("isGoldenRecord", Some(BooleanToYNConverter)),
      SOURCE = getValue("sourceName"),
      SOURCE_ID = getValue("sourceEntityId"),
      CREATED_AT = getValue("ohubCreated"),
      UPDATED_AT = getValue("ohubUpdated"),
      DELETE_FLAG = getValue("isActive", Some(InvertedBooleanToYNConverter)),
      NAME = getValue("name", Some(CleanString)),
      CHANNEL = getValue("channel"),
      SUB_CHANNEL = getValue("subChannel"),
      REGION = getValue("region"),
      OTM = getValue("otm"),
      PREFERRED_PARTNER = getValue("distributorName", Some(CleanString)),
      STREET = getValue("street"),
      HOUSE_NUMBER = getValue("houseNumber"),
      HOUSE_NUMBER_EXT = getValue("houseNumberExtension"),
      CITY = getValue("city"),
      COUNTRY = getValue("countryName"),
      ZIP_CODE = getValue("zipCode"),
      AVERAGE_SELLING_PRICE = getValue("averagePrice"),
      NUMBER_OF_COVERS = getValue("totalDishes"),
      NUMBER_OF_WEEKS_OPEN = getValue("weeksClosed", Some(WeeksClosedToOpened)),
      NUMBER_OF_DAYS_OPEN = getValue("daysOpen"),
      STATUS = getValue("isActive", Some(BooleanToADConverter)),
      CONVENIENCE_LEVEL = getValue("cookingConvenienceLevel"),
      RESPONSIBLE_EMPLOYEE = getValue("salesRepresentative"),
      NPS_POTENTIAL = getValue("netPromoterScore"),
      CHANNEL_TEXT = getValue("channel"),
      CHAIN_KNOTEN = getValue("chainId"),
      CHAIN_NAME = getValue("chainName", Some(CleanString)),
      DM_OPT_OUT = getValue("hasDirectMailOptOut", Some(BooleanToYNConverter)),
      EMAIL_OPT_OUT = getValue("hasEmailOptOut", Some(BooleanToYNConverter)),
      FIXED_OPT_OUT = getValue("hasTelemarketingOptOut", Some(BooleanToYNConverter)),
      MOBILE_OPT_OUT = getValue("hasMobileOptOut", Some(BooleanToYNConverter)),
      FAX_OPT_OUT = getValue("hasFaxOptOut", Some(BooleanToYNConverter)),
      KITCHEN_TYPE = getValue("kitchenType", Some(CleanString)),
      STATE = getValue("state"),
      WHOLE_SALER_OPERATOR_ID = getValue("distributorOperatorId"),
      PRIVATE_HOUSEHOLD = getValue("isPrivateHousehold", Some(BooleanToYNConverter)),
      VAT = getValue("vat"),
      OPEN_ON_MONDAY = getValue("isOpenOnMonday", Some(BooleanTo10Converter)),
      OPEN_ON_TUESDAY = getValue("isOpenOnTuesday", Some(BooleanTo10Converter)),
      OPEN_ON_WEDNESDAY = getValue("isOpenOnWednesday", Some(BooleanTo10Converter)),
      OPEN_ON_THURSDAY = getValue("isOpenOnThursday", Some(BooleanTo10Converter)),
      OPEN_ON_FRIDAY = getValue("isOpenOnFriday", Some(BooleanTo10Converter)),
      OPEN_ON_SATURDAY = getValue("isOpenOnSaturday", Some(BooleanTo10Converter)),
      OPEN_ON_SUNDAY = getValue("isOpenOnSunday", Some(BooleanTo10Converter)),
      LOCAL_CHANNEL = getValue("localChannel"),
      CHANNEL_USAGE = getValue("channelUsage"),
      SOCIAL_COMMERCIAL = getValue("socialCommercial"),
      STRATEGIC_CHANNEL = getValue("strategicChannel"),
      GLOBAL_CHANNEL = getValue("globalChannel"),
      GLOBAL_SUBCHANNEL = getValue("globalSubChannel")
    )
  }
}
