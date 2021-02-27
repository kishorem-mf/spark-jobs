package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

import com.unilever.ohub.spark.datalake.OperatorGoldenOutboundWriter
import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import com.unilever.ohub.spark.domain.{DomainEntity, DomainEntityCompanion}
import com.unilever.ohub.spark.export.ExportOutboundWriter
import com.unilever.ohub.spark.export.azuredw.{AzureDWWriter, OperatorGoldenDWWriter}
import com.unilever.ohub.spark.export.ddl.OperatorDdlOutboundWriter
import com.unilever.ohub.spark.export.domain.DomainExportWriter

object OperatorGoldenDomainExportWriter extends DomainExportWriter[OperatorGolden]

object OperatorGolden extends DomainEntityCompanion[OperatorGolden] {
  override val auroraFolderLocation = Some("Restricted")
  override val engineFolderName = "operators_golden"
  override val domainExportWriter: Option[DomainExportWriter[OperatorGolden]] = Some(OperatorGoldenDomainExportWriter)
  override val acmExportWriter: Option[ExportOutboundWriter[OperatorGolden]] = None
  override val dispatchExportWriter: Option[ExportOutboundWriter[OperatorGolden]] = None
  override val azureDwWriter: Option[AzureDWWriter[OperatorGolden]] = Some(OperatorGoldenDWWriter)
  override val auroraInboundWriter: Option[ExportOutboundWriter[OperatorGolden]] = None
  override val ddlExportWriter: Option[ExportOutboundWriter[OperatorGolden]] = Some(OperatorDdlOutboundWriter)
}


case class OperatorGolden(
                     // generic fields
                     id: String,
                     creationTimestamp: Timestamp,
                     concatId: String,
                     countryCode: String,
                     customerType: String,
                     dateCreated: Option[Timestamp],
                     dateUpdated: Option[Timestamp],
                     isActive: Boolean,
                     isGoldenRecord: Boolean,
                     ohubId: Option[String],
                     name: Option[String],
                     sourceEntityId: String,
                     sourceName: String,
                     ohubCreated: Timestamp,
                     ohubUpdated: Timestamp,
                     // specific fields
                     annualTurnover: Option[BigDecimal],
                     averagePrice: Option[BigDecimal],
                     averageRating: Option[Int],
                     beveragePurchasePotential: Option[BigDecimal],
                     buildingSquareFootage: Option[String],
                     chainId: Option[String],
                     chainName: Option[String],
                     channel: Option[String],
                     city: Option[String],
                     cookingConvenienceLevel: Option[String],
                     countryName: Option[String],
                     daysOpen: Option[Int],
                     distributorName: Option[String],
                     distributorOperatorId: Option[String],
                     emailAddress: Option[String],
                     faxNumber: Option[String],
                     hasDirectMailOptIn: Option[Boolean],
                     hasDirectMailOptOut: Option[Boolean],
                     hasEmailOptIn: Option[Boolean],
                     hasEmailOptOut: Option[Boolean],
                     hasFaxOptIn: Option[Boolean],
                     hasFaxOptOut: Option[Boolean],
                     hasGeneralOptOut: Option[Boolean],
                     hasMobileOptIn: Option[Boolean],
                     hasMobileOptOut: Option[Boolean],
                     hasTelemarketingOptIn: Option[Boolean],
                     hasTelemarketingOptOut: Option[Boolean],
                     headQuarterAddress: Option[String],
                     headQuarterCity: Option[String],
                     headQuarterPhoneNumber: Option[String],
                     headQuarterState: Option[String],
                     headQuarterZipCode: Option[String],
                     houseNumber: Option[String],
                     houseNumberExtension: Option[String],
                     isNotRecalculatingOtm: Option[Boolean],
                     isOpenOnFriday: Option[Boolean],
                     isOpenOnMonday: Option[Boolean],
                     isOpenOnSaturday: Option[Boolean],
                     isOpenOnSunday: Option[Boolean],
                     isOpenOnThursday: Option[Boolean],
                     isOpenOnTuesday: Option[Boolean],
                     isOpenOnWednesday: Option[Boolean],
                     isPrivateHousehold: Option[Boolean],
                     kitchenType: Option[String],
                     menuKeywords: Option[String],
                     mobileNumber: Option[String],
                     netPromoterScore: Option[BigDecimal],
                     numberOfProductsFittingInMenu: Option[Int],
                     numberOfReviews: Option[Int],
                     oldIntegrationId: Option[String],
                     operatorLeadScore: Option[Int],
                     otm: Option[String],
                     otmEnteredBy: Option[String],
                     phoneNumber: Option[String],
                     potentialSalesValue: Option[BigDecimal],
                     region: Option[String],
                     salesRepresentative: Option[String],
                     state: Option[String],
                     street: Option[String],
                     subChannel: Option[String],
                     totalDishes: Option[Int],
                     totalLocations: Option[Int],
                     totalStaff: Option[Int],
                     vat: Option[String],
                     wayOfServingAlcohol: Option[String],
                     website: Option[String],
                     webUpdaterId: Option[String],
                     weeksClosed: Option[Int],
                     yearFounded: Option[Int],
                     zipCode: Option[String],
                     localChannel: Option[String],
                     channelUsage: Option[String],
                     socialCommercial: Option[String],
                     strategicChannel: Option[String],
                     globalChannel: Option[String],
                     globalSubChannel: Option[String],
                     ufsClientNumber: Option[String],
                     department: Option[String],
                     // other fields
                     additionalFields: Map[String, String],
                     ingestionErrors: Map[String, IngestionError]
                   ) extends DomainEntity {
  override def getCompanion: DomainEntityCompanion[OperatorGolden] = OperatorGolden
}
