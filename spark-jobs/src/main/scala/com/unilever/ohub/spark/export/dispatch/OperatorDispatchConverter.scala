package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.export.dispatch.model.DispatchOperator
import com.unilever.ohub.spark.export.{Converter, TransformationFunctions}

object OperatorDispatchConverter extends Converter[Operator, DispatchOperator] with TransformationFunctions with DispatchTransformationFunctions {

  override def convert(operator: Operator): DispatchOperator = {
    DispatchOperator(
      COUNTRY_CODE = operator.countryCode,
      OPR_ORIG_INTEGRATION_ID = operator.concatId,
      OPR_LNKD_INTEGRATION_ID = operator.ohubId,
      GOLDEN_RECORD_FLAG = operator.isGoldenRecord,
      SOURCE = operator.sourceName,
      SOURCE_ID = operator.sourceEntityId,
      CREATED_AT = operator.ohubCreated,
      UPDATED_AT = operator.ohubUpdated,
      DELETE_FLAG = !operator.isActive,
      NAME = cleanString(operator.name),
      CHANNEL = operator.channel,
      SUB_CHANNEL = operator.subChannel,
      REGION = operator.region,
      OTM = operator.otm,
      PREFERRED_PARTNER = cleanString(operator.distributorName),
      STREET = operator.street,
      HOUSE_NUMBER = operator.houseNumber,
      HOUSE_NUMBER_EXT = operator.houseNumberExtension,
      CITY = operator.city,
      COUNTRY = operator.countryName,
      ZIP_CODE = operator.zipCode,
      AVERAGE_SELLING_PRICE = operator.averagePrice,
      NUMBER_OF_COVERS = operator.totalDishes,
      NUMBER_OF_WEEKS_OPEN = operator.weeksClosed,
      NUMBER_OF_DAYS_OPEN = operator.daysOpen,
      STATUS = operator.isActive,
      CONVENIENCE_LEVEL = operator.cookingConvenienceLevel,
      RESPONSIBLE_EMPLOYEE = operator.salesRepresentative,
      NPS_POTENTIAL = operator.netPromoterScore,
      CHANNEL_TEXT = operator.channel,
      CHAIN_KNOTEN = operator.chainId,
      CHAIN_NAME = cleanString(operator.chainName),
      DM_OPT_OUT = operator.hasDirectMailOptOut,
      EMAIL_OPT_OUT = operator.hasEmailOptOut,
      FIXED_OPT_OUT = operator.hasTelemarketingOptOut,
      MOBILE_OPT_OUT = operator.hasMobileOptOut,
      FAX_OPT_OUT = operator.hasFaxOptOut,
      KITCHEN_TYPE = cleanString(operator.kitchenType),
      STATE = operator.state,
      WHOLE_SALER_OPERATOR_ID = operator.distributorOperatorId,
      PRIVATE_HOUSEHOLD = operator.isPrivateHousehold,
      VAT = operator.vat,
      OPEN_ON_MONDAY = operator.isOpenOnMonday,
      OPEN_ON_TUESDAY = operator.isOpenOnTuesday,
      OPEN_ON_WEDNESDAY = operator.isOpenOnWednesday,
      OPEN_ON_THURSDAY = operator.isOpenOnThursday,
      OPEN_ON_FRIDAY = operator.isOpenOnFriday,
      OPEN_ON_SATURDAY = operator.isOpenOnSaturday,
      OPEN_ON_SUNDAY = operator.isOpenOnSunday,
      LOCAL_CHANNEL = operator.localChannel,
      CHANNEL_USAGE = operator.channelUsage,
      SOCIAL_COMMERCIAL = operator.socialCommercial,
      STRATEGIC_CHANNEL = operator.strategicChannel,
      GLOBAL_CHANNEL = operator.globalChannel,
      GLOBAL_SUBCHANNEL = operator.globalSubChannel
    )
  }
}
