package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import com.unilever.ohub.spark.domain.constraint.ConcatIdConstraintTypes.SourceEntityId


object OrderLine {
  val customerType = "ORDERLINE"
}

case class OrderLine(
                      // generic fields
                      concatId: String,
                      countryCode: String,
                      customerType: String,
                      dateCreated: Option[Timestamp],
                      dateUpdated: Option[Timestamp],
                      isActive: Boolean,
                      isGoldenRecord: Boolean,
                      sourceEntityId: String,
                      sourceName: String,
                      ohubId: Option[String],
                      ohubCreated: Timestamp,
                      ohubUpdated: Timestamp,
                      // specific fields
                      orderConcatId: String,
                      quantityOfUnits: Option[Long],
                      amount: Option[BigDecimal],
                      pricePerUnit: Option[BigDecimal],
                      currency: Option[String],
                      // other fields
                      additionalFields: Map[String, String],
                      ingestionErrors: Map[String, IngestionError]
                    ) extends DomainEntity {
}
