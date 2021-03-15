package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.Order
import com.unilever.ohub.spark.ingest.CustomParsers._
import com.unilever.ohub.spark.ingest.{DomainTransformer, OrderEmptyParquetWriter}
import org.apache.spark.sql.Row

object OrderConverter extends CommonDomainGateKeeper[Order] with OrderEmptyParquetWriter {

  // scalastyle:off method.length
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
        ufsClientNumber = optional("ufsClientNumber"),
        deliveryType = optional("deliveryType"),
        preferredDeliveryDateOption = optional("preferredDeliveryDateOption"),
        preferredDeliveryDate = optional("preferredDeliveryDate", parseDateTimeUnsafe()),
        department = optional("department"),
        //BDL fields
        createdBy = optional("createdBy"),
        createdBySap = optional("createdBySap", toBoolean),
        customerHierarchyLevel3 = optional("customerHierarchyLevel3"),
        customerHierarchyLevel4 = optional("customerHierarchyLevel4"),
        customerHierarchyLevel5 = optional("customerHierarchyLevel5"),
        customerHierarchyLevel6 = optional("customerHierarchyLevel6"),
        customerNameLevel3 = optional("customerNameLevel3"),
        customerNameLevel4 = optional("customerNameLevel4"),
        customerNameLevel5 = optional("customerNameLevel5"),
        customerNameLevel6 = optional("customerNameLevel6"),
        deliveryStatus = optional("deliveryStatus"),
        discount = optional("discount", toDecimal),
        externalCustomerHierarchyLevel1 = optional("externalCustomerHierarchyLevel1"),
        externalCustomerHierarchyLevel1Description = optional("externalCustomerHierarchyLevel1Description"),
        externalCustomerHierarchyLevel2 = optional("externalCustomerHierarchyLevel2"),
        externalCustomerHierarchyLevel2Description = optional("externalCustomerHierarchyLevel2Description"),
        invoiceNumber = optional("invoiceNumber"),
        netInvoiceValue = optional("netInvoiceValue", toDecimal),
        opportunityOwner = optional("opportunityOwner"),
        orderCreationDate = optional("orderCreationDate", parseDateTimeUnsafe()),
        purchaseOrderNumber = optional("purchaseOrderNumber"),
        purchaseOrderType = optional("purchaseOrderType"),
        rejectionStatus = optional("rejectionStatus"),
        sellInOrSellOut = optional("sellInOrSellOut"),
        stageOfCompletion = optional("stageOfCompletion"),
        totalGrossPrice = optional("totalGrossPrice", toDecimal),
        totalSurcharge = optional("totalSurcharge", toDecimal),
        wholesalerDistributionCenter = optional("wholesalerDistributionCenter"),
        wholesalerSellingPriceBasedAmount = optional("wholesalerSellingPriceBasedAmount", toDecimal),
        // other fields
        additionalFields = additionalFields,
        ingestionErrors = errors
      )
  }
}
