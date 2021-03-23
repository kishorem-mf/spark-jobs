package com.unilever.ohub.spark.domain.entity

import java.sql.{Date, Timestamp}

import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import com.unilever.ohub.spark.domain.{DomainEntity, DomainEntityCompanion}
import com.unilever.ohub.spark.export.ExportOutboundWriter
import com.unilever.ohub.spark.export.azuredw.{AzureDWWriter, ContactPersonGoldenDWWriter}
import com.unilever.ohub.spark.export.businessdatalake.{AzureDLWriter, ContactPersonGoldenDLWriter}
import com.unilever.ohub.spark.export.ddl.ContactPersonDdlOutboundWriter
import com.unilever.ohub.spark.export.domain.DomainExportWriter

object ContactPersonGoldenDomainExportWriter extends DomainExportWriter[ContactPersonGolden]

object ContactPersonGolden extends DomainEntityCompanion[ContactPersonGolden] {
  override val auroraFolderLocation = Some("Restricted")
  override val engineFolderName = "contactpersons_golden"
  override val domainExportWriter: Option[DomainExportWriter[ContactPersonGolden]] = Some(ContactPersonGoldenDomainExportWriter)
  override val acmExportWriter: Option[ExportOutboundWriter[ContactPersonGolden]] = None
  override val dispatchExportWriter: Option[ExportOutboundWriter[ContactPersonGolden]] = None
  override val azureDwWriter: Option[AzureDWWriter[ContactPersonGolden]] = Some(ContactPersonGoldenDWWriter)
  override val auroraInboundWriter: Option[ExportOutboundWriter[ContactPersonGolden]] = None
  override val dataLakeWriter: Option[AzureDLWriter[ContactPersonGolden]] = Some(ContactPersonGoldenDLWriter)
  override val ddlExportWriter: Option[ExportOutboundWriter[ContactPersonGolden]] = Some(ContactPersonDdlOutboundWriter)
}

case class ContactPersonGolden(
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
                          //crm fields
                          crmId:Option[String],
                          otherJobTitle:Option[String],
                          optInSourceName:Option[String],
                          subscriptionsList:Option[String],
                          executeRightToBeForgotten:Option[Boolean],
                          hasTelephoneSuppressed:Option[Boolean],
                          emailOptOutDate:Option[Timestamp],
                          startWorkDate:Option[Timestamp],
                          endWorkDate:Option[Timestamp],
                          doNotCall:Option[Boolean],
                          emailOptInStatus:Option[String],
                          emailConsentDate:Option[Timestamp],
                          mobileOptOutDate:Option[Timestamp],
                          mobileOptInStatus:Option[String],
                          mobileConsentDate:Option[Timestamp],
                          teleMarketingOptInDate:Option[Timestamp],
                          hasTeleMarketingDoubleOptIn:Option[Boolean],
                          teleMarketingDoubleOptInDate:Option[Timestamp],
                          teleMarketingOptOutDate:Option[Timestamp],
                          teleMarketingOptInStatus:Option[String],
                          teleMarketingConsentDate:Option[Timestamp],
                          hasCalculatedEmailConsent:Option[Boolean],
                          hasCalculatedMobileConsent:Option[Boolean],
                          hasCalculatedTeleMarketingConsent:Option[Boolean],
                          // other fields
                          additionalFields: Map[String, String],
                          ingestionErrors: Map[String, IngestionError]
                        ) extends DomainEntity {
  override def getCompanion: DomainEntityCompanion[ContactPersonGolden] = ContactPersonGolden
}
