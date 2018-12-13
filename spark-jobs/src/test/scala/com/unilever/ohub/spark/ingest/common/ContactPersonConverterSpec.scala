package com.unilever.ohub.spark.ingest.common

import java.sql.{ Date, Timestamp }

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.ingest.{ CsvDomainConfig, CsvDomainGateKeeperSpec }
import com.unilever.ohub.spark.storage.DefaultStorage
import org.apache.spark.sql.Dataset

class ContactPersonConverterSpec extends CsvDomainGateKeeperSpec[ContactPerson] {

  override val SUT = ContactPersonConverter

  describe("common contact person converter") {
    it("should convert a contact person correctly from a valid common csv input") {
      val inputFile = "src/test/resources/COMMON_CONTACTPERSONS.csv"
      val config = CsvDomainConfig(inputFile = inputFile, outputFile = "", fieldSeparator = ";")

      runJobWith(config) { actualDataSet ⇒
        actualDataSet.count() shouldBe 1

        val actualContactPerson = actualDataSet.head()
        val expectedContactPerson =
          ContactPerson(
            id = "id-1",
            creationTimestamp = new Timestamp(1542205922011L),
            concatId = "AU~WUFOO~AB123",
            countryCode = "AU",
            customerType = "CONTACTPERSON",
            dateCreated = Some(Timestamp.valueOf("2015-06-30 13:47:00.0")),
            dateUpdated = Some(Timestamp.valueOf("2015-06-30 13:48:00.0")),
            isActive = true,
            isGoldenRecord = false,
            ohubId = None,
            sourceEntityId = "AB123",
            sourceName = "WUFOO",
            ohubCreated = actualContactPerson.ohubCreated,
            ohubUpdated = actualContactPerson.ohubUpdated,
            operatorConcatId = "AU~WUFOO~E1-1234",
            operatorOhubId = None,
            oldIntegrationId = Some("G1234"),
            firstName = Some("John"),
            lastName = Some("Williams"),
            title = Some("Mr"),
            gender = Some("M"),
            jobTitle = Some("Chef"),
            language = Some("en"),
            birthDate = Some(Date.valueOf("1975-12-21")),
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
            phoneNumber = Some("+61 3 9662 1811"),
            mobileNumber = Some("+61 6 1234 5678"),
            faxNumber = Some("+61 3 9662 1811"),
            hasGeneralOptOut = Some(true),
            hasRegistration = Some(true),
            registrationDate = Some(Timestamp.valueOf("2015-09-30 14:23:00.0")),
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

    it("should write a contact person parquet correctly from an emtpy csv input and remain readable") {
      val inputFile = "src/test/resources/empty.csv"
      val outputFile = "src/test/resources/output/contact_person_with_schema.parquet"
      val config = CsvDomainConfig(inputFile = inputFile, outputFile = outputFile, fieldSeparator = "‰")
      val storage = new DefaultStorage(spark)

      SUT.run(spark, config, storage)

      val result: Dataset[ContactPerson] = storage.readFromParquet[ContactPerson](outputFile)
      result.collect().toSeq shouldBe Seq[ContactPerson]()
    }
  }
}
