package com.unilever.ohub.spark.tsv2parquet.file_interface

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.tsv2parquet.CsvDomainGateKeeperSpec

class ContactPersonConverterSpec extends CsvDomainGateKeeperSpec[ContactPerson] {

  private[tsv2parquet] override val SUT = ContactPersonConverter

  describe("file interface contact person converter") {
    it("should convert a contact person correctly from a valid file interface csv input") {
      val inputFile = "src/test/resources/FILE_CONTACTPERSONS.csv"

      runJobWith(inputFile) { actualDataSet ⇒
        actualDataSet.count() shouldBe 1

        val actualContactPerson = actualDataSet.head()
        val expectedContactPerson =
          ContactPerson(
            concatId = "AU~WUFOO~AB123",
            countryCode = "AU",
            customerType = "CONTACTPERSON",
            dateCreated = Some(Timestamp.valueOf("2015-06-30 13:47:00.0")),
            dateUpdated = Some(Timestamp.valueOf("2015-06-30 13:48:00.0")),
            isActive = true,
            isGoldenRecord = false,
            ohubId = None,
            name = "TODO",
            sourceEntityId = "AB123",
            sourceName = "WUFOO",
            ohubCreated = actualContactPerson.ohubCreated,
            ohubUpdated = actualContactPerson.ohubUpdated,
            operatorConcatId = "AU~WUFOO~E1-1234",
            oldIntegrationId = Some("G1234"),
            firstName = Some("John"),
            lastName = Some("Williams"),
            title = Some("Mr"),
            gender = Some("M"),
            jobTitle = Some("Chef"),
            language = Some("en"),
            birthDate = Some(Timestamp.valueOf("1975-12-21 00:00:00.0")),
            street = Some("Highstreet"),
            houseNumber = Some("443"),
            houseNumberExtension = Some("A"),
            city = Some("Melbourne"),
            zipCode = Some("2057"),
            state = Some("Alabama"),
            countryName = Some("Australia"),
            isPreferredContact = Some(true),
            isKeyDecisionMaker = Some(true),
            standardCommunicationChannel = Some("Mobile"),
            emailAddress = Some("jwilliams@downunder.au"),
            phoneNumber = Some("61396621811"),
            mobileNumber = Some("61612345678"),
            faxNumber = Some("61396621811"),
            hasGeneralOptOut = Some(true),
            hasConfirmedRegistration = Some(true),
            confirmedRegistrationDate = Some(Timestamp.valueOf("2015-09-30 14:23:00.0")),
            hasEmailOptIn = Some(true),
            emailOptInDate = Some(Timestamp.valueOf("2015-09-30 14:23:00.0")),
            hasEmailDoubleOptIn = Some(true),
            emailDoubleOptInDate = Some(Timestamp.valueOf("2015-09-30 14:23:00.0")),
            hasEmailOptOut = Some(true),
            hasDirectMailOptIn = Some(true),
            hasDirectMailOptOut = Some(true),
            hasTeleMarketingOptIn = Some(true),
            hasTeleMarketingOptOut = Some(true),
            hasMobileOptIn = Some(true),
            mobileOptInDate = Some(Timestamp.valueOf("2015-09-30 14:23:00.0")),
            hasMobileDoubleOptIn = Some(true),
            mobileDoubleOptInDate = Some(Timestamp.valueOf("2015-09-30 14:23:00.0")),
            hasMobileOptOut = Some(true),
            hasFaxOptIn = Some(true),
            hasFaxOptOut = Some(true),
            webUpdaterId = None,
            additionalFields = Map(),
            ingestionErrors = Map()
          )

        actualContactPerson shouldBe expectedContactPerson
      }
    }
  }
}
