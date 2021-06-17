package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

object TestWholesalerAssignments extends TestWholesalerAssignments

trait TestWholesalerAssignments {

  // format: OFF
  lazy val defaultWholesalerAssignment: WholesalerAssignment = WholesalerAssignment(
    id = "id-1",
    creationTimestamp = new Timestamp(1542205922011L),
    concatId = "country-code~source-name~source-entity-id",
    countryCode = "country-code",
    customerType = "OPERATOR",
    dateCreated = None,
    dateUpdated = None,
    isActive = true,
    isGoldenRecord = true,
    ohubId = Some("ohub-id"),
    operatorOhubId = None,
    operatorConcatId = Some("some~operator~id"),
    sourceEntityId = "sourceEntityId",
    sourceName = "sourceName",
    ohubCreated = Timestamp.valueOf("2015-06-30 13:49:00.0"),
    ohubUpdated = Timestamp.valueOf("2015-06-30 13:49:00.0"),
    routeToMarketIceCreamCategory = Some("routeToMarketIceCreamCategory"),
    isPrimaryFoodsWholesaler = Some(true),
    isPrimaryIceCreamWholesaler = Some(true),
    isPrimaryFoodsWholesalerCrm = Some(true),
    isPrimaryIceCreamWholesalerCrm = Some(true),
    wholesalerCustomerCode2 = Some("wholesalerCustomerCode2"),
    wholesalerCustomerCode3 = Some("wholesalerCustomerCode3"),
    hasPermittedToShareSsd = Some(true),
    isProvidedByCrm = Some(true),
    crmId = Some("crmId"),
    additionalFields = Map(),
    ingestionErrors = Map()
  )


  // format: ON
}
