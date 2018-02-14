package com.unilever.ohub.spark.data

import java.sql.Timestamp

  case class EmOrderRecord(
    orderId: Option[Long],
    webServiceRequestId: Option[Long],
    countryCode: Option[String], // enum
    orderType: Option[String], // enum: Web
    transactionDate: Option[Timestamp],
    orderAmount: Option[Long], // 0
    currencyCode: Option[String], // empty
    wholesaler: Option[String],
    orderUid: Option[String],
    campaignCode: Option[String],
    campaignName: Option[String],
    street: Option[String],
    houseNumber: Option[String],
    houseNumberAdd: Option[String], // empty
    zipCode: Option[String],
    city: Option[String],
    state: Option[String], // empty
    country: Option[String], // empty
    refOrderId: Option[String], // empty
    orderEmailAddress: Option[String], // empty
    orderPhoneNumber: Option[String], // empty
    orderMobilePhoneNumber: Option[String], // empty
    wholesalerLocation: Option[String], // empty
    wholesalerId: Option[String], // empty
    wholesalerCustomerNumber: Option[String], // empty
    invoiceName: Option[String], // empty
    invoiceStreet: Option[String], // empty
    invoiceHouseNumber: Option[String], // empty
    invoiceHouseNumberAdd: Option[String], // empty
    invoiceZipCode: Option[String], // empty
    invoiceCity: Option[String], // empty
    invoiceState: Option[String], // empty
    invoiceCountry: Option[String], // empty
    vat: Option[String], // empty
    comments: Option[String]) // empty
