package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import com.unilever.ohub.spark.domain.{DomainEntity, DomainEntityCompanion}
import com.unilever.ohub.spark.export.domain.DomainExportWriter

object ChainDomainExportWriter extends DomainExportWriter[Chain]

object Chain extends DomainEntityCompanion {
  val customerType = "CHAIN"
  override val engineFolderName: String = "chains"
  override val domainExportWriter: Option[DomainExportWriter[Chain]] = Some(ChainDomainExportWriter)
}

case class Chain(
                  id: String,
                  creationTimestamp: Timestamp,
                  concatId: String,
                  countryCode: String,
                  customerType: String = Chain.customerType,
                  dateCreated: Option[Timestamp],
                  dateUpdated: Option[Timestamp],
                  isActive: Boolean,
                  isGoldenRecord: Boolean,
                  ohubId: Option[String],
                  sourceEntityId: String,
                  sourceName: String,
                  ohubCreated: Timestamp,
                  ohubUpdated: Timestamp,
                  conceptName: Option[String],
                  numberOfUnits: Option[Int],
                  numberOfStates: Option[Int],
                  estimatedAnnualSales: Option[BigDecimal],
                  estimatedPurchasePotential: Option[BigDecimal],
                  address: Option[String],
                  city: Option[String],
                  state: Option[String],
                  zipCode: Option[String],
                  website: Option[String],
                  phone: Option[String],
                  segment: Option[String],
                  primaryMenu: Option[String],
                  secondaryMenu: Option[String],
                  additionalFields: Map[String, String],
                  ingestionErrors: Map[String, IngestionError]
                ) extends DomainEntity {
  override def getCompanion: DomainEntityCompanion = Chain
}

