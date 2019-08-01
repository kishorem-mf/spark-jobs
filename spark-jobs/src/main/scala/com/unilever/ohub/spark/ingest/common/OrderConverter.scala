package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.Order
import com.unilever.ohub.spark.ingest.CustomParsers._
import com.unilever.ohub.spark.ingest.{DomainTransformer, OrderEmptyParquetWriter}
import org.apache.spark.sql.Row

object OrderConverter extends CommonDomainGateKeeper[Order] with OrderEmptyParquetWriter {

  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ Order = { transformer ⇒
    row ⇒
      import transformer._
      implicit val source: Row = row

      val ohubCreated = new Timestamp(System.currentTimeMillis())


      Order(
        id = mandatory("id"),
        creationTimestamp = mandatory("creationTimestamp", toTimestamp),
        concatId = mandatory("concatId"),
        countryCode = mandatory("countryCode"),
        customerType = Order.customerType,
        dateCreated = optional("dateCreated", parseDateTimeUnsafe()),
        dateUpdated = optional("dateUpdated", parseDateTimeUnsafe()),
        isActive = mandatory("isActive", toBoolean),
        isGoldenRecord = false, // set in OrderMerging
        ohubId = None, // set in OrderMerging
        sourceEntityId = mandatory("sourceEntityId"),
        sourceName = mandatory("sourceName"),
        ohubCreated = ohubCreated,
        ohubUpdated = ohubCreated,
        // specific fields
        orderUid = optional("orderUid"),
        `type` = mandatory("orderType"),
        campaignCode = optional("campaignCode"),
        campaignName = optional("campaignName"),
        comment = optional("comment"),
        contactPersonConcatId = optional("contactPersonConcatId"),
        contactPersonOhubId = None, // set in OrderMerging
        distributorId = optional("distributorId"),
        distributorLocation = optional("distributorLocation"),
        distributorName = optional("distributorName"),
        distributorOperatorId = optional("distributorOperatorId"),
        currency = optional("currency"),
        operatorConcatId = optional("operatorConcatId"),
        operatorOhubId = None, // set in OrderMerging
        transactionDate = optional("transactionDate", parseDateTimeUnsafe()),
        vat = optional("vat", toBigDecimal),
        amount = optional("amount", toBigDecimal),

        // invoice address
        invoiceOperatorName = optional("invoiceOperatorName"),
        invoiceOperatorStreet = optional("invoiceOperatorStreet"),
        invoiceOperatorHouseNumber = optional("invoiceOperatorHouseNumber"),
        invoiceOperatorHouseNumberExtension = optional("invoiceOperatorHouseNumberExtension"),
        invoiceOperatorZipCode = optional("invoiceOperatorZipCode"),
        invoiceOperatorCity = optional("invoiceOperatorCity"),
        invoiceOperatorState = optional("invoiceOperatorState"),
        invoiceOperatorCountry = optional("invoiceOperatorCountry"),
        // delivery address
        deliveryOperatorName = optional("deliveryOperatorName"),
        deliveryOperatorStreet = optional("deliveryOperatorStreet"),
        deliveryOperatorHouseNumber = optional("deliveryOperatorHouseNumber"),
        deliveryOperatorHouseNumberExtension = optional("deliveryOperatorHouseNumberExtension"),
        deliveryOperatorZipCode = optional("deliveryOperatorZipCode"),
        deliveryOperatorCity = optional("deliveryOperatorCity"),
        deliveryOperatorState = optional("deliveryOperatorState"),
        deliveryOperatorCountry = optional("deliveryOperatorCountry"),
        // other fields
        additionalFields = additionalFields,
        ingestionErrors = errors
      )
  }
}
