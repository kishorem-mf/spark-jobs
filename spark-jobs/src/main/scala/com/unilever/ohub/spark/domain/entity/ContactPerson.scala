package com.unilever.ohub.spark.domain.entity

import java.sql.{Date, Timestamp}

import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import com.unilever.ohub.spark.domain.{DomainEntity, DomainEntityCompanion}
import com.unilever.ohub.spark.export.domain.DomainExportWriter

object ContactPersonDomainExportWriter extends DomainExportWriter[ContactPerson]

object ContactPerson extends DomainEntityCompanion {
  val customerType = "CONTACTPERSON"
  override val engineFolderName = "contactpersons"
  override val domainExportWriter: Option[DomainExportWriter[ContactPerson]] = Some(ContactPersonDomainExportWriter)
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
                          isEmailAddressValid: Option[Boolean],

                          // other fields
                          additionalFields: Map[String, String],
                          ingestionErrors: Map[String, IngestionError]
                        ) extends DomainEntity {
  override def getCompanion: DomainEntityCompanion = ContactPerson
}

case class InvalidEmail(emailAddress: String)

