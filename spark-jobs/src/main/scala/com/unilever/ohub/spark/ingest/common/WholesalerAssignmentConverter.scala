package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.WholesalerAssignment
import com.unilever.ohub.spark.ingest.CustomParsers._
import com.unilever.ohub.spark.ingest.{WholesalerAssignmentEmptyParquetWriter, DomainTransformer}
import org.apache.spark.sql.Row

object WholesalerAssignmentConverter extends CommonDomainGateKeeper[WholesalerAssignment] with WholesalerAssignmentEmptyParquetWriter {

  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ WholesalerAssignment = { transformer ⇒
    row ⇒
      import transformer._
      implicit val source: Row = row

      val ohubCreated = new Timestamp(System.currentTimeMillis())

      WholesalerAssignment(
        id = mandatory("id"),
        creationTimestamp = mandatory("creationTimestamp", toTimestamp),
        concatId = mandatory("concatId"),
        countryCode = mandatory("countryCode"),
        customerType = mandatory("customerType"),
        dateCreated = optional("dateCreated", parseDateTimeUnsafe()),
        dateUpdated = optional("dateUpdated", parseDateTimeUnsafe()),
        isActive = mandatory("isActive", toBoolean),
        isGoldenRecord = false,
        ohubId = Option.empty,
        operatorOhubId = optional("operatorOhubId"),
        operatorConcatId = optional("operatorConcatId"),
        sourceEntityId = mandatory("sourceEntityId"),
        sourceName = mandatory("sourceName"),
        ohubCreated = ohubCreated,
        ohubUpdated = ohubCreated,
        routeToMarketIceCreamCategory = optional("routeToMarketIceCreamCategory"),
        isPrimaryFoodsWholesaler = optional("isPrimaryFoodsWholesaler", toBoolean),
        isPrimaryIceCreamWholesaler = optional("isPrimaryIceCreamWholesaler", toBoolean),
        isPrimaryFoodsWholesalerCrm = optional("isPrimaryFoodsWholesalerCrm", toBoolean),
        isPrimaryIceCreamWholesalerCrm = optional("isPrimaryIceCreamWholesalerCrm", toBoolean),
        wholesalerCustomerCode2 = optional("wholesalerCustomerCode2"),
        wholesalerCustomerCode3 = optional("wholesalerCustomerCode3"),
        hasPermittedToShareSsd = optional("hasPermittedToShareSsd", toBoolean),
        isProvidedByCrm = optional("isProvidedByCrm", toBoolean),
        crmId = optional("crmId"),
        additionalFields = additionalFields,
        ingestionErrors = errors
      )
  }
}
