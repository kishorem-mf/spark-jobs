package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.OrderLine
import com.unilever.ohub.spark.ingest.CustomParsers._
import com.unilever.ohub.spark.ingest.{DomainTransformer, OrderLineEmptyParquetWriter}
import org.apache.spark.sql.Row

object OrderLineConverter extends CommonDomainGateKeeper[OrderLine] with OrderLineEmptyParquetWriter {

  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ OrderLine = { transformer ⇒
    row ⇒
      import transformer._
      implicit val source: Row = row

      val ohubCreated = new Timestamp(System.currentTimeMillis())

      OrderLine(
        id = mandatory("id"),
        creationTimestamp = mandatory("creationTimestamp", toTimestamp),
        concatId = mandatory("concatId"),
        countryCode = mandatory("countryCode"),
        customerType = OrderLine.customerType,
        dateCreated = optional("dateCreated", parseDateTimeUnsafe()),
        dateUpdated = optional("dateUpdated", parseDateTimeUnsafe()),
        isActive = mandatory("isActive", toBoolean),
        isGoldenRecord = true,
        ohubId = None, // set in OrderLineMerging
        sourceEntityId = mandatory("sourceEntityId"),
        sourceName = mandatory("sourceName"),
        ohubCreated = ohubCreated,
        ohubUpdated = ohubCreated,
        orderConcatId = mandatory("orderConcatId"),
        productConcatId = mandatory("productConcatId"),
        productSourceEntityId = mandatory("productRefId"),
        comment = optional("comment"),
        quantityOfUnits = mandatory("quantityOfUnits", toInt),
        amount = mandatory("amount", toBigDecimal),
        pricePerUnit = optional("pricePerUnit", toBigDecimal),
        currency = optional("currency"),
        campaignLabel = optional("campaignLabel"),
        loyaltyPoints = optional("loyaltyPoints", toBigDecimal),
        productOhubId = None, // set in OrderLineMerging
        orderType = optional("orderType"),
        //BDL Fields
        discount = optional("discount", toBigDecimal),
        discountPercentage = optional("discountPercentage", toBigDecimal),
        distributorProductCode = optional("distributorProductCode"),
        freeOfCharge = optional("freeOfCharge", toBoolean),
        lineNumber = optional("lineNumber"),
        materialNetWeight = optional("materialNetWeight", toBigDecimal),
        netInvoiceValue = optional("netInvoiceValue", toBigDecimal),
        salesPrice = optional("salesPrice", toBigDecimal),
        unitOfMeasure = optional("unitOfMeasure"),
        volumeCasesSold = optional("volumeCasesSold", toBigDecimal),
        wholesalerSellingPrice = optional("wholesalerSellingPrice", toBigDecimal),
        additionalFields = additionalFields,
        ingestionErrors = errors
      )
  }
}
