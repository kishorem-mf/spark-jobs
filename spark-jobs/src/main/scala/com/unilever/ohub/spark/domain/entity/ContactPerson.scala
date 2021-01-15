package com.unilever.ohub.spark.domain.entity

import java.sql.{Date, Timestamp}

import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import com.unilever.ohub.spark.domain.{DomainEntity, DomainEntityCompanion}
import com.unilever.ohub.spark.export.ExportOutboundWriter
import com.unilever.ohub.spark.export.azuredw.{AzureDWWriter, ContactPersonDWWriter}
import com.unilever.ohub.spark.export.domain.DomainExportWriter

object ContactPerson extends DomainEntityCompanion[ContactPerson] {
  val customerType = "CONTACTPERSON"
  override val auroraFolderLocation = Some("Restricted")
  override val engineFolderName = "contactpersons"
  override val engineGoldenFolderName = Some("contactpersons_golden")
  override val domainExportWriter: Option[DomainExportWriter[ContactPerson]] = Some(com.unilever.ohub.spark.export.domain.ContactPersonDomainExportWriter)
  override val acmExportWriter: Option[ExportOutboundWriter[ContactPerson]] = Some(com.unilever.ohub.spark.export.acm.ContactPersonOutboundWriter)
  override val dispatchExportWriter: Option[ExportOutboundWriter[ContactPerson]] = Some(com.unilever.ohub.spark.export.dispatch.ContactPersonOutboundWriter)
  override val azureDwWriter: Option[AzureDWWriter[ContactPerson]] = Some(ContactPersonDWWriter)
  override val auroraInboundWriter: Option[ExportOutboundWriter[ContactPerson]] = Some(com.unilever.ohub.spark.datalake.ContactPersonOutboundWriter)
}

case class ContactPerson(
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
                          sourceEntityId: String,
                          sourceName: String,
                          ohubCreated: Timestamp,
                          ohubUpdated: Timestamp,
                          operatorConcatId: Option[String],
                          operatorOhubId: Option[String],
                          // specific fields
                          oldIntegrationId: Option[String],
                          firstName: Option[String],
                          lastName: Option[String],
                          title: Option[String],
                          gender: Option[String],
                          jobTitle: Option[String],
                          language: Option[String],
                          birthDate: Option[Date],
                          street: Option[String],
                          houseNumber: Option[String],
                          houseNumberExtension: Option[String],
                          city: Option[String],
                          zipCode: Option[String],
                          state: Option[String],
                          countryName: Option[String],
                          isPreferredContact: Option[Boolean],
                          isKeyDecisionMaker: Option[Boolean],
                          standardCommunicationChannel: Option[String],
                          emailAddress: Option[String],
                          phoneNumber: Option[String],
                          mobileNumber: Option[String],
                          faxNumber: Option[String],
                          hasGeneralOptOut: Option[Boolean],
                          hasConfirmedRegistration: Option[Boolean],
                          hasRegistration: Option[Boolean],
                          registrationDate: Option[Timestamp],
                          confirmedRegistrationDate: Option[Timestamp],
                          hasEmailOptIn: Option[Boolean],
                          emailOptInDate: Option[Timestamp],
                          hasEmailDoubleOptIn: Option[Boolean],
                          emailDoubleOptInDate: Option[Timestamp],
                          hasEmailOptOut: Option[Boolean],
                          hasDirectMailOptIn: Option[Boolean],
                          hasDirectMailOptOut: Option[Boolean],
                          hasTeleMarketingOptIn: Option[Boolean],
                          hasTeleMarketingOptOut: Option[Boolean],
                          hasMobileOptIn: Option[Boolean],
                          mobileOptInDate: Option[Timestamp],
                          hasMobileDoubleOptIn: Option[Boolean],
                          mobileDoubleOptInDate: Option[Timestamp],
                          hasMobileOptOut: Option[Boolean],
                          hasFaxOptIn: Option[Boolean],
                          hasFaxOptOut: Option[Boolean],
                          webUpdaterId: Option[String],
                          socialNetworkName: Option[String],
                          socialNetworkId: Option[String],
                          isEmailAddressValid: Option[Boolean],
                          isMobileNumberValid: Option[Boolean],

                          // other fields
                          additionalFields: Map[String, String],
                          ingestionErrors: Map[String, IngestionError]
                        ) extends DomainEntity {
  override def getCompanion: DomainEntityCompanion[ContactPerson] = ContactPerson
}

case class InvalidEmail(emailAddress: String)

case class InvalidMobile(mobileNumber: String)

