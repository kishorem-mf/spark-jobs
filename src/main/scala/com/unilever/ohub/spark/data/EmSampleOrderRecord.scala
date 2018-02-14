package com.unilever.ohub.spark.data

  case class EmSampleOrderRecord(
    sampleOrderId: Option[Long],
    countryCode: Option[String], // enum
    webServiceRequestId: Option[Long],
    itemType: Option[String], // enum: Sample
    termsAgreed: Option[Boolean], // 1
    itemId: Option[Long],
    orderItem: Option[String], // enum: Product
    quantity: Option[Long], // 1
    street: Option[String],
    houseNumber: Option[String],
    houseNumberExt: Option[String], // empty
    postcode: Option[String],
    city: Option[String],
    state: Option[String], // empty
    country: Option[String])
