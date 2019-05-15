package com.unilever.ohub.spark.export.acm

import com.unilever.ohub.spark.DomainDataProvider
import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.export.acm.model.AcmOperator
import com.unilever.ohub.spark.export.{Converter, TransformationFunctions}

object OperatorAcmConverter extends Converter[Operator, AcmOperator] with TransformationFunctions with AcmTransformationFunctions {

  override def convert(op: Operator): AcmOperator = {
    AcmOperator(
      OPR_ORIG_INTEGRATION_ID = op.ohubId.get,
      OPR_LNKD_INTEGRATION_ID = new OperatorOldOhubConverter(DomainDataProvider().sourceIds).convert(op.concatId),
      GOLDEN_RECORD_FLAG = "Y",
      COUNTRY_CODE = op.countryCode,
      NAME = op.name.map(cleanString),
      CHANNEL = op.channel,
      SUB_CHANNEL = op.subChannel,
      REGION = op.region,
      OTM = op.otm,
      PREFERRED_PARTNER = op.distributorName.map(cleanString),
      STREET = op.street,
      HOUSE_NUMBER = op.houseNumber,
      ZIPCODE = op.zipCode,
      CITY = op.city,
      COUNTRY = op.countryName,
      AVERAGE_SELLING_PRICE = op.averagePrice,
      NUMBER_OF_COVERS = op.totalDishes,
      NUMBER_OF_WEEKS_OPEN = op.weeksClosed.map { weeksClosed ⇒
        if (52 - weeksClosed < 0) 0 else 52 - weeksClosed
      },
      NUMBER_OF_DAYS_OPEN = op.daysOpen,
      CONVENIENCE_LEVEL = op.cookingConvenienceLevel,
      RESPONSIBLE_EMPLOYEE = op.salesRepresentative,
      NPS_POTENTIAL = op.netPromoterScore,
      CHAIN_KNOTEN = op.chainId,
      CHAIN_NAME = op.chainName.map(cleanString),
      DATE_CREATED = op.dateCreated,
      DATE_UPDATED = op.dateUpdated,
      DELETE_FLAG = !op.isActive,
      WHOLESALER_OPERATOR_ID = op.distributorOperatorId,
      PRIVATE_HOUSEHOLD = op.isPrivateHousehold,
      VAT = op.vat,
      OPEN_ON_MONDAY = op.isOpenOnMonday,
      OPEN_ON_TUESDAY = op.isOpenOnTuesday,
      OPEN_ON_WEDNESDAY = op.isOpenOnWednesday,
      OPEN_ON_THURSDAY = op.isOpenOnThursday,
      OPEN_ON_FRIDAY = op.isOpenOnFriday,
      OPEN_ON_SATURDAY = op.isOpenOnSaturday,
      OPEN_ON_SUNDAY = op.isOpenOnSunday,
      KITCHEN_TYPE = op.kitchenType.map(cleanString),
      LOCAL_CHANNEL = op.localChannel,
      CHANNEL_USAGE = op.channelUsage,
      SOCIAL_COMMERCIAL = op.socialCommercial,
      STRATEGIC_CHANNEL = op.strategicChannel,
      GLOBAL_CHANNEL = op.globalChannel,
      GLOBAL_SUBCHANNEL = op.globalSubChannel
    )
  }
}
