package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.domain.entity.Chain
import com.unilever.ohub.spark.export.{Converter, TypeConversionFunctions}
import com.unilever.ohub.spark.export.dispatch.model.DispatchChain

object ChainDispatchConverter extends Converter[Chain, DispatchChain] with DispatchTransformationFunctions {

  override def convert(chain: Chain): DispatchChain = {
    DispatchChain(
      CHAIN_INTEGRATION_ID = chain.concatId ,
      SOURCE = chain.sourceName,
      COUNTRY_CODE = chain.countryCode,
      CUSTOMER_TYPE = chain.customerType ,
      CREATED_AT = chain.ohubCreated,
      UPDATED_AT = chain.ohubUpdated,
      DELETE_FLAG = booleanToYNConverter(!chain.isActive) ,

      CONCEPT_NAME = chain.conceptName ,
      NUM_OF_UNITS = chain.numberOfUnits ,
      NUM_OF_STATES = chain.numberOfStates ,
      ESTIMATED_ANNUAL_SALES = chain.estimatedAnnualSales ,
      ESTIMATED_PURCHASE_POTENTIAL = chain.estimatedPurchasePotential ,
      ADDRESS = chain.address,
      CITY = chain.city,
      STATE = chain.state,
      ZIP_CODE = chain.zipCode,
      WEBSITE = chain.website,
      PHONE_NUMBER = chain.phone,
      SEGMENT = chain.segment ,
      PRIMARY_MENU = chain.primaryMenu ,
      SECONDARY_MENU = chain.secondaryMenu
    )
  }
}
