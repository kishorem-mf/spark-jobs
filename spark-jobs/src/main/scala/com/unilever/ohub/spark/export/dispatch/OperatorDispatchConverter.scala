package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.export._
import com.unilever.ohub.spark.export.dispatch.model.DispatchOperator

private[export] object BooleanToADConverter extends TransformationFunction[Boolean] {

  def impl(input: Boolean):String = if (input) "A" else "D"

  val description: String = "Converts a boolean: true -> A(ctive) or  false -> D(eactivated)"
}

object OperatorDispatchConverter extends Converter[Operator, DispatchOperator] with TypeConversionFunctions with DispatchTypeConversionFunctions {

  // scalastyle:off method.length
  override def convert(implicit operator: Operator, explain: Boolean = false): DispatchOperator = {
    DispatchOperator(
      COUNTRY_CODE = getValue("countryCode"),
      OPR_ORIG_INTEGRATION_ID = getValue("concatId"),
      OPR_LNKD_INTEGRATION_ID = getValue("ohubId"),
      GOLDEN_RECORD_FLAG = getValue("isGoldenRecord", BooleanToYNConverter),
      SOURCE = getValue("sourceName"),
      SOURCE_ID = getValue("sourceEntityId"),
      CREATED_AT = getValue("ohubCreated"),
      UPDATED_AT = getValue("ohubUpdated"),
      DELETE_FLAG = getValue("isActive", InvertedBooleanToYNConverter),
      NAME = getValue("name", CleanString),
      CHANNEL = getValue("channel"),
      SUB_CHANNEL = getValue("subChannel"),
      REGION = getValue("region"),
      OTM = getValue("otm"),
      PREFERRED_PARTNER = getValue("distributorName", CleanString),
      STREET = getValue("street"),
      HOUSE_NUMBER = getValue("houseNumber"),
      HOUSE_NUMBER_EXT = getValue("houseNumberExtension"),
      CITY = getValue("city"),
      COUNTRY = getValue("countryName"),
      ZIP_CODE = getValue("zipCode"),
      AVERAGE_SELLING_PRICE = getValue("averagePrice"),
      NUMBER_OF_COVERS = getValue("totalDishes"),
      NUMBER_OF_WEEKS_OPEN = getValue("weeksClosed", WeeksClosedToOpened),
      NUMBER_OF_DAYS_OPEN = getValue("daysOpen"),
      STATUS = getValue("isActive", BooleanToADConverter),
      CONVENIENCE_LEVEL = getValue("cookingConvenienceLevel"),
      RESPONSIBLE_EMPLOYEE = getValue("salesRepresentative"),
      NPS_POTENTIAL = getValue("netPromoterScore"),
      CHANNEL_TEXT = getValue("channel"),
      CHAIN_KNOTEN = getValue("chainId"),
      CHAIN_NAME = getValue("chainName", CleanString),
      DM_OPT_OUT = getValue("hasDirectMailOptOut", BooleanToYNUConverter),
      EMAIL_OPT_OUT = getValue("hasEmailOptOut", BooleanToYNUConverter),
      FIXED_OPT_OUT = getValue("hasTelemarketingOptOut", BooleanToYNUConverter),
      MOBILE_OPT_OUT = getValue("hasMobileOptOut", BooleanToYNUConverter),
      FAX_OPT_OUT = getValue("hasFaxOptOut", BooleanToYNUConverter),
      KITCHEN_TYPE = getValue("kitchenType", CleanString),
      STATE = getValue("state"),
      WHOLE_SALER_OPERATOR_ID = getValue("distributorOperatorId"),
      PRIVATE_HOUSEHOLD = getValue("isPrivateHousehold", BooleanToYNConverter),
      VAT = getValue("vat"),
      OPEN_ON_MONDAY = getValue("isOpenOnMonday", BooleanTo10Converter),
      OPEN_ON_TUESDAY = getValue("isOpenOnTuesday", BooleanTo10Converter),
      OPEN_ON_WEDNESDAY = getValue("isOpenOnWednesday", BooleanTo10Converter),
      OPEN_ON_THURSDAY = getValue("isOpenOnThursday", BooleanTo10Converter),
      OPEN_ON_FRIDAY = getValue("isOpenOnFriday", BooleanTo10Converter),
      OPEN_ON_SATURDAY = getValue("isOpenOnSaturday", BooleanTo10Converter),
      OPEN_ON_SUNDAY = getValue("isOpenOnSunday", BooleanTo10Converter),
      LOCAL_CHANNEL = getValue("localChannel"),
      CHANNEL_USAGE = getValue("channelUsage"),
      SOCIAL_COMMERCIAL = getValue("socialCommercial"),
      STRATEGIC_CHANNEL = getValue("strategicChannel"),
      GLOBAL_CHANNEL = getValue("globalChannel"),
      GLOBAL_SUBCHANNEL = getValue("globalSubChannel"),
      UFS_CLIENT_NUMBER = getValue("ufsClientNumber")
    )
  }
}
