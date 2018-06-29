package com.unilever.ohub.spark.tsv2parquet.web_event

import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.tsv2parquet.CsvDomainGateKeeperSpec
import org.apache.spark.sql.Dataset
import cats.syntax.option._

class ContactPersonConverterSpec extends CsvDomainGateKeeperSpec[ContactPerson] {
  override private[tsv2parquet] val SUT = ContactPersonConverter

  describe("web event contact person converter") {
    it("should convert an contact person correctly from a valid web event csv input") {
      val inputFile = "src/test/resources/WEB_EVENT_CONTACT_PERSONS.csv"

      runJobWith(inputFile) { actualDataSet: Dataset[ContactPerson] ⇒
        actualDataSet.count() shouldBe 1
        val actual: ContactPerson = actualDataSet.head()
        val expected = ContactPerson(
          concatId = "NL~EMAKINA~sourceId",
          countryCode = "NL",
          customerType = "CONTACTPERSON",
          dateCreated = none,
          dateUpdated = none,
          isActive = true,
          isGoldenRecord = false,
          ohubId = none,
          name = "firstName lastName",
          sourceEntityId = "sourceId",
          sourceName = "EMAKINA",
          ohubCreated = actual.ohubCreated,
          ohubUpdated = actual.ohubUpdated,
          operatorConcatId = "NL~EMAKINA~sourceId",
          operatorOhubId = none,
          oldIntegrationId = none,
          firstName = "firstName",
          lastName = "lastName",
          title = "title",
          gender = "M",
          jobTitle = "jobTitle",
          language = "en",
          birthDate = none,
          street = none,
          houseNumber = none,
          houseNumberExtension = none,
          city = none,
          zipCode = none,
          state = none,
          countryName = "Netherlands",
          isPreferredContact = none,
          isKeyDecisionMaker = none,
          standardCommunicationChannel = none,
          emailAddress = "email@address.nl",
          phoneNumber = "3161396621911", // after cleaning and formatting
          mobileNumber = "3161612345678", // after cleaning and formatting
          faxNumber = "3161396621811", // after cleaning and formatting
          hasGeneralOptOut = none,
          hasConfirmedRegistration = none,
          confirmedRegistrationDate = none,
          hasEmailOptIn = none,
          emailOptInDate = none,
          hasEmailDoubleOptIn = none,
          emailDoubleOptInDate = none,
          hasEmailOptOut = none,
          hasDirectMailOptIn = none,
          hasDirectMailOptOut = none,
          hasTeleMarketingOptIn = none,
          hasTeleMarketingOptOut = none,
          hasMobileOptIn = none,
          mobileOptInDate = none,
          hasMobileDoubleOptIn = none,
          mobileDoubleOptInDate = none,
          hasMobileOptOut = none,
          hasFaxOptIn = none,
          hasFaxOptOut = none,
          webUpdaterId = none,
          additionalFields = Map(),
          ingestionErrors = Map()
        )

        actual shouldBe expected
      }
    }
  }
}
