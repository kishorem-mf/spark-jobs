package com.unilever.ohub.spark.tsv2parquet.emakina

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.tsv2parquet.DomainGateKeeperCsvSpec

class ContactPersonConverterSpec extends DomainGateKeeperCsvSpec[ContactPerson] {

  private[tsv2parquet] override val SUT = ContactPersonConverter

  describe("emakina contact person converter") {
    it("should convert a contact person correctly from a valid emakina csv input") {
      val inputFile = "src/test/resources/EMAKINA_CONTACT_PERSONS.csv"

      runJobWith(inputFile) { actualDataSet ⇒
        actualDataSet.count() shouldBe 1

        val actualContactPerson = actualDataSet.head()
        val expectedContactPerson =
          ContactPerson(
            concatId = "DE~EMAKINA~b3a6208c-d7f6-44e2-80e2-f26d461f64c0",
            countryCode = "DE",
            customerType = "CONTACTPERSON",
            dateCreated = None,
            dateUpdated = None,
            isActive = true,
            isGoldenRecord = false,
            ohubId = None,
            name = "TODO",
            sourceEntityId = "b3a6208c-d7f6-44e2-80e2-f26d461f64c0",
            sourceName = "EMAKINA",
            ohubCreated = actualContactPerson.ohubCreated,
            ohubUpdated = actualContactPerson.ohubUpdated,
            operatorConcatId = "DE~EMAKINA~my ref id",
            oldIntegrationId = None,
            firstName = Some("Anika"),
            lastName = Some("Henke"),
            title = Some("my title"),
            gender = Some("U"),
            jobTitle = Some("my job title"),
            language = Some("DE"),
            birthDate = None,
            street = Some("Am Schloßhof"),
            houseNumber = Some("14"),
            houseNumberExtension = Some("my extension"),
            city = Some("Berlin"),
            zipCode = Some("12683"),
            state = Some("my state"),
            countryName = Some("Germany"),
            isPreferredContact = None,
            isKeyDecisionMaker = None,
            standardCommunicationChannel = None,
            emailAddress = Some("anika.henke@gmx.de"),
            phoneNumber = Some("491735421197"),
            mobileNumber = Some("49"),
            faxNumber = Some("49"),
            hasGeneralOptOut = None,
            hasConfirmedRegistration = None,
            confirmedRegistrationDate = None,
            hasEmailOptIn = None,
            emailOptInDate = Some(Timestamp.valueOf("2017-05-25 18:41:07.0")),
            hasEmailDoubleOptIn = Some(true),
            emailDoubleOptInDate = Some(Timestamp.valueOf("2017-05-25 18:41:07.0")),
            hasEmailOptOut = None,
            hasDirectMailOptIn = None,
            hasDirectMailOptOut = None,
            hasTeleMarketingOptIn = None,
            hasTeleMarketingOptOut = None,
            hasMobileOptIn = None,
            mobileOptInDate = None,
            hasMobileDoubleOptIn = None,
            mobileDoubleOptInDate = None,
            hasMobileOptOut = None,
            hasFaxOptIn = None,
            hasFaxOptOut = None,
            webUpdaterId = Some("my webupdater id"),
            additionalFields = Map(),
            ingestionErrors = Map()
          )

        actualContactPerson shouldBe expectedContactPerson
      }
    }
  }
}
