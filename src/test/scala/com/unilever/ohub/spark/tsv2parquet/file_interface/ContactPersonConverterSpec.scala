package com.unilever.ohub.spark.tsv2parquet.file_interface

import java.io.File
import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.storage.DefaultStorage
import com.unilever.ohub.spark.tsv2parquet.{ CsvDomainGateKeeperSpec, PostgressDomainDataProvider }
import com.unilever.ohub.spark.tsv2parquet.DomainGateKeeper.DomainConfig
import com.unilever.ohub.spark.SharedSparkSession.spark
import ru.yandex.qatools.embed.postgresql.EmbeddedPostgres
import java.sql.Connection
import java.sql.DriverManager

class ContactPersonConverterSpec extends CsvDomainGateKeeperSpec[ContactPerson] {

  private[tsv2parquet] override val SUT = ContactPersonConverter

  describe("file interface contact person converter") {
    it("should convert a contact person correctly from a valid file interface csv input") {
      val inputFile = "src/test/resources/FILE_CONTACTPERSONS.csv"
      val config = DomainConfig(inputFile = inputFile, outputFile = "", fieldSeparator = "‰")

      runJobWith(config) { actualDataSet ⇒
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
            name = "John Williams",
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

    it("should write a contact person parquet correctly from an emtpy csv input") {
      val inputFile = "src/test/resources/empty.csv"
      val outputFile = "src/test/resources/output/contact_person_with_schema.parquet"
      val db = "dbName"
      val user = "userName"
      val pwd = "password"

      val postgres = new EmbeddedPostgres()
      val url = postgres.start("localhost", 5432, db, user, pwd)
      val conn = DriverManager.getConnection(url)
      conn.createStatement.execute(
        """
          |CREATE TABLE all_country_info (
          |ISO3166_1_Alpha_2 char(5),
          |official_name_en char(5),
          |ISO4217_currency_alphabetic_code char(5)
          |);""".stripMargin)
      conn.createStatement.execute(
        """
          |CREATE TABLE country_codes (
          |COUNTRY_CODE char(5),
          |SALES_ORG char(5)
          |);""".stripMargin)
      conn.createStatement.execute(
        """CREATE TABLE
          |data_sources (
          |SOURCE char(5),
          |PRIORITY char(5)
          |);""".stripMargin)
      conn.createStatement.execute(
        """CREATE TABLE
          |channel_mapping (
          |channel_reference_fk char(5),
          |COUNTRY_CODE char(5),
          |ORIGINAL_CHANNEL char(5),
          |LOCAL_CHANNEL char(5),
          |CHANNEL_USAGE char(5),
          |SOCIAL_COMMERCIAL char(5),
          |STRATEGIC_CHANNEL char(5),
          |GLOBAL_CHANNEL char(5),
          |GLOBAL_SUBCHANNEL char(5)
          |);""".stripMargin)
      conn.createStatement.execute(
        """CREATE TABLE
          |channel_reference_id (
          |channel_reference_id char(5)
          |);""".stripMargin)

      val config = DomainConfig(
        inputFile = inputFile,
        outputFile = outputFile,
        fieldSeparator = "‰",
        postgressUrl = url,
        postgressUsername = user,
        postgressPassword = pwd,
        postgressDB = db
      )

      OperatorConverter.run(spark, config, new DefaultStorage(spark))

      postgres.stop()

      assert(new File(outputFile).list.exists(_.contains("part")))
    }
  }
}
