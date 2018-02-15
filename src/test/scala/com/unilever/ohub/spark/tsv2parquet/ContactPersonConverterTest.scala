package com.unilever.ohub.spark.tsv2parquet

import java.sql.Timestamp

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.data.{ ContactPersonRecord, CountryRecord }

class ContactPersonConverterTest extends SparkJobSpec {
  private val defaultContactPersonRecord = ContactPersonRecord(
    contactPersonConcatId = "",
    refContactPersonId = Some(""),
    source = Some("Source"),
    countryCode = Some(""),
    status = Some(true),
    statusOriginal = Some("true"),
    refOperatorId = Some(""),
    contactPersonIntegrationId = Some(""),
    dateCreated = Some(new Timestamp(1)),
    dateModified = Some(new Timestamp(1)),
    firstName = Some(""),
    firstNameCleansed = Some(""),
    lastName = Some(""),
    lastNameCleansed = Some(""),
    bothNamesCleansed = Some(""),
    title = Some(""),
    gender = Some(""),
    function = Some(""),
    languageKey = Some(""),
    birthDate = Some(new Timestamp(1)),
    street = Some(""),
    streetCleansed = Some(""),
    houseNumber = Some(""),
    houseNumberExt = Some(""),
    city = Some(""),
    cityCleansed = Some(""),
    zipCode = Some(""),
    zipCodeCleansed = Some(""),
    state = Some(""),
    country = Some("Foo"),
    preferredContact = Some(true),
    preferredContactOriginal = Some(""),
    keyDecisionMaker = Some(true),
    keyDecisionMakerOriginal = Some(""),
    scm = Some(""),
    emailAddress = Some(""),
    emailAddressOriginal = Some(""),
    phoneNumber = Some(""),
    phoneNumberOriginal = Some(""),
    mobilePhoneNumber = Some(""),
    mobilePhoneNumberOriginal = Some(""),
    faxNumber = Some(""),
    optOut = Some(true),
    optOutOriginal = Some(""),
    registrationConfirmed = Some(true),
    registrationConfirmedOriginal = Some(""),
    registrationConfirmedDate = Some(new Timestamp(1)),
    registrationConfirmedDateOriginal = Some(""),
    emailOptIn = Some(true),
    emailOptInOriginal = Some(""),
    emailOptInDate = Some(new Timestamp(1)),
    emailOptInDateOriginal = Some(""),
    emailOptInConfirmed = Some(true),
    emailOptInConfirmedOriginal = Some(""),
    emailOptInConfirmedDate = Some(new Timestamp(1)),
    emailOptInConfirmedDateOriginal = Some(""),
    emailOptOut = Some(true),
    emailOptOutOriginal = Some(""),
    directMailOptIn = Some(true),
    directMailOptInOriginal = Some(""),
    directMailOptOut = Some(true),
    directMailOptOutOriginal = Some(""),
    telemarketingOptIn = Some(true),
    telemarketingOptInOriginal = Some(""),
    telemarketingOptOut = Some(true),
    telemarketingOptOutOriginal = Some(""),
    mobileOptIn = Some(true),
    mobileOptInOriginal = Some(""),
    mobileOptInDate = Some(new Timestamp(1)),
    mobileOptInDateOriginal = Some(""),
    mobileOptInConfirmed = Some(true),
    mobileOptInConfirmedOriginal = Some(""),
    mobileOptInConfirmedDate = Some(new Timestamp(1)),
    mobileOptInConfirmedDateOriginal = Some(""),
    mobileOptOut = Some(true),
    mobileOptOutOriginal = Some(""),
    faxOptIn = Some(true),
    faxOptInOriginal = Some(""),
    faxOptOut = Some(true),
    faxOptOutOriginal = Some("")
  )

  import spark.implicits._

  describe("The ContactPersonConverter job's") {
    describe("transform function") {
      describe("when given a ContactPersonRecord with COUNTRY_CODE 'NL'") {
        val contactPersonRecord = defaultContactPersonRecord.copy(
          countryCode = Some("NL")
        )

        describe("and given a CountryRecord with COUNTRY_CODE 'NL'") {
          val countryRecord = CountryRecord(
            countryCode = "NL",
            countryName = "Netherlands",
            currencyCode = "EUR"
          )

          it("should add the 'Netherlands' as COUNTRY to the ContactPersonRecord") {
            val result = ContactPersonConverter.transform(
              spark,
              contactPersonRecord.toDataset,
              countryRecord.toDataset
            ).head()

            result.country.get shouldEqual "Netherlands"
          }
        }
      }
    }
  }
}
