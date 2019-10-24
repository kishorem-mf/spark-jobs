package com.unilever.ohub.spark.export.acm

import java.io.{BufferedReader, File, InputStreamReader}
import java.util.UUID

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.domain.entity.TestContactPersons
import com.unilever.ohub.spark.export.TargetType
import com.unilever.ohub.spark.export.acm.ContactPersonOutboundWriter.getDeletedOhubIdsWithTargetId
import com.unilever.ohub.spark.export.domain.InMemStorage
import com.unilever.ohub.spark.{SparkJobSpec, export}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.{Assertion, BeforeAndAfter, Matchers}
import org.spark_project.dmg.pmml.True

class AcmContactPersonsExportWriterSpec extends SparkJobSpec with TestContactPersons with BeforeAndAfter with Matchers {

  import spark.implicits._

  val storage = new InMemStorage(spark, contactPersons, prevIntegrated)
  val previousMergedDs = Seq(
    defaultContactPerson.copy(isGoldenRecord = true, ohubId = Some("3211"), concatId = "AU~OHUB~3211"),
    defaultContactPerson.copy(isGoldenRecord = true, ohubId = Some("5555"), concatId = "AU~OHUB~5555")
  ).toDataset
  val mergedDs = Seq(
    defaultContactPerson.copy(isGoldenRecord = true, ohubId = Some("3211"), concatId = "AU~OHUB~3211"),
    defaultContactPerson.copy(isGoldenRecord = true, ohubId = Some("5555"), concatId = "AU~OHUB~5555"),
    defaultContactPerson.copy(isGoldenRecord = true, ohubId = Some("8888"), concatId = "AU~OHUB~8888", firstName = Some("New incoming Record"))
  ).toDataset
  private val changedCP = defaultContactPerson.copy(isGoldenRecord = true, ohubId = Some("1234"), concatId = "AU~WUFOO~1234")
  private val unchangedCP = defaultContactPerson.copy(isGoldenRecord = true, concatId = "AU~WUFOO~2345")
  private val SUT = com.unilever.ohub.spark.export.acm.ContactPersonOutboundWriter
  private val contactPersons = Seq(changedCP, unchangedCP, defaultContactPerson).toDataset
  private val prevIntegrated = Seq(unchangedCP, defaultContactPerson).toDataset
  private val outboundLocation = UUID.randomUUID().toString
  private val config = export.OutboundConfig(
    integratedInputFile = "integrated",
    outboundLocation = outboundLocation,
    targetType = TargetType.ACM
  )

  after {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.delete(new Path(config.outboundLocation), true)
  }

  describe("ACM csv generation") {
    it("Should write correct csv") {
      SUT.export(contactPersons, getDeletedOhubIdsWithTargetId(spark, prevIntegrated, contactPersons, previousMergedDs, mergedDs).unionByName, mergedDs, previousMergedDs, config, spark)

      val result = storage.readFromCsv(config.outboundLocation, new AcmOptions {}.delimiter, true)

      result.collect().length shouldBe 1
      result.collect().head.toString() should include("AU~8888~3~183")
    }

    it("Should fitler based on coutryCodes") {
      val configWithCountries = config.copy(countryCodes = Some(Seq("NL")))

      SUT.export(contactPersons, getDeletedOhubIdsWithTargetId(spark, prevIntegrated, contactPersons, previousMergedDs, mergedDs).unionByName, mergedDs, previousMergedDs, configWithCountries, spark)

      val result = storage.readFromCsv(config.outboundLocation, new AcmOptions {}.delimiter, true)

      result.collect().length shouldBe 0
    }

    it("Should contain header") {
      SUT.export(contactPersons, getDeletedOhubIdsWithTargetId(spark, prevIntegrated, contactPersons, previousMergedDs, mergedDs).unionByName, mergedDs, previousMergedDs, config, spark)

      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val csvFile = fs.listStatus(new Path(config.outboundLocation)).find(status => status.getPath.getName.contains("UFS_RECIPIENTS")).get
      val header = readFirstLine(csvFile.getPath, fs)
      header shouldBe "CP_ORIG_INTEGRATION_ID¶CP_LNKD_INTEGRATION_ID¶OPR_ORIG_INTEGRATION_ID¶GOLDEN_RECORD_FLAG¶WEB_CONTACT_ID¶EMAIL_OPTOUT¶PHONE_OPTOUT¶FAX_OPTOUT¶MOBILE_OPTOUT¶DM_OPTOUT¶LAST_NAME¶FIRST_NAME¶MIDDLE_NAME¶TITLE¶GENDER¶LANGUAGE¶EMAIL_ADDRESS¶MOBILE_PHONE_NUMBER¶PHONE_NUMBER¶FAX_NUMBER¶STREET¶HOUSENUMBER¶ZIPCODE¶CITY¶COUNTRY¶DATE_CREATED¶DATE_UPDATED¶DATE_OF_BIRTH¶PREFERRED¶ROLE¶COUNTRY_CODE¶SCM¶DELETE_FLAG¶KEY_DECISION_MAKER¶OPT_IN¶OPT_IN_DATE¶CONFIRMED_OPT_IN¶CONFIRMED_OPT_IN_DATE¶MOB_OPT_IN¶MOB_OPT_IN_DATE¶MOB_CONFIRMED_OPT_IN¶MOB_CONFIRMED_OPT_IN_DATE¶MOB_OPT_OUT_DATE¶ORG_FIRST_NAME¶ORG_LAST_NAME¶ORG_EMAIL_ADDRESS¶ORG_FIXED_PHONE_NUMBER¶ORG_MOBILE_PHONE_NUMBER¶ORG_FAX_NUMBER¶HAS_REGISTRATION¶REGISTRATION_DATE¶HAS_CONFIRMED_REGISTRATION¶CONFIRMED_REGISTRATION_DATE¶SOURCE_IDS¶TARGET_OHUB_ID"
    }

    it("Should write the conversionMapping in json format") {
      val mappingLocation = new Path(outboundLocation, "contactperson-mapping.json")
      SUT.export(contactPersons, getDeletedOhubIdsWithTargetId(spark, prevIntegrated, contactPersons, previousMergedDs, mergedDs).unionByName,
        mergedDs, previousMergedDs, config.copy(mappingOutputLocation = Some(mappingLocation.toString)), spark)

      val mapper = new ObjectMapper() with ScalaObjectMapper
      mapper.registerModule(DefaultScalaModule)

      val conversionMapping = mapper.readTree(new File(mappingLocation.toString))
      conversionMapping.size() > 50 shouldBe true
      conversionMapping.get("REGISTRATION_DATE").size() shouldBe 8
    }

    it("should export new incoming merged ohubids to ACM") {

      val prevIntegratedDS = Seq(
        defaultContactPerson.copy(isGoldenRecord = true, ohubId = Some("5555"), concatId = "AU~WUFOO~2345"),
        defaultContactPerson.copy(isGoldenRecord = true, ohubId = Some("5555"), concatId = "AU~WUFOO~0000")
      ).toDataset

      val integratedDs = Seq(
        defaultContactPerson.copy(isGoldenRecord = true, ohubId = Some("3211"), concatId = "AU~WUFOO~1234"),
        defaultContactPerson.copy(isGoldenRecord = true, ohubId = Some("5555"), concatId = "AU~WUFOO~2345"),
        defaultContactPerson.copy(isGoldenRecord = true, ohubId = Some("5555"), concatId = "AU~WUFOO~0000"),
        defaultContactPerson.copy(isGoldenRecord = true, ohubId = Some("8888"), concatId = "AU~WUFOO~4321", firstName = Some("New incoming Record"))
      ).toDataset

      val previousMergedDs = Seq(
        defaultContactPerson.copy(isGoldenRecord = true, ohubId = Some("3211"), concatId = "AU~OHUB~3211"),
        defaultContactPerson.copy(isGoldenRecord = true, ohubId = Some("5555"), concatId = "AU~OHUB~5555")
      ).toDataset

      val mergedDs = Seq(
        defaultContactPerson.copy(isGoldenRecord = true, ohubId = Some("3211"), concatId = "AU~OHUB~3211"),
        defaultContactPerson.copy(isGoldenRecord = true, ohubId = Some("5555"), concatId = "AU~OHUB~5555"),
        defaultContactPerson.copy(isGoldenRecord = true, ohubId = Some("8888"), concatId = "AU~OHUB~8888", firstName = Some("New incoming Record"))
      ).toDataset

      SUT.export(integratedDs, getDeletedOhubIdsWithTargetId(spark, prevIntegratedDS, integratedDs, previousMergedDs, mergedDs).unionByName, mergedDs, previousMergedDs, config, spark)

      val result: Dataset[Row] = storage.readFromCsv(config.outboundLocation, new AcmOptions {}.delimiter, true)

      result.collect().length shouldBe 1
      result.filter($"CP_ORIG_INTEGRATION_ID" === "8888").select($"CP_LNKD_INTEGRATION_ID").collect().head.toString() should include("AU~8888~3~183")
    }

    it("should export only new merged ohubids to ACM when there are no deleted records required") {

      val prevIntegratedDs = Seq(
        defaultContactPerson.copy(concatId = "AU~WUFOO~101", ohubId = Some("1"), isGoldenRecord = true),
        defaultContactPerson.copy(concatId = "AU~WUFOO~102", ohubId = Some("1")),
        defaultContactPerson.copy(concatId = "AU~WUFOO~103", ohubId = Some("2"), isGoldenRecord = true),
        defaultContactPerson.copy(concatId = "AU~WUFOO~104", ohubId = Some("2"))
      ).toDataset

      val integratedDs = Seq(
        defaultContactPerson.copy(concatId = "AU~WUFOO~101", ohubId = Some("1"), isGoldenRecord = true),
        defaultContactPerson.copy(concatId = "AU~WUFOO~102", ohubId = Some("3")),
        defaultContactPerson.copy(concatId = "AU~WUFOO~103", ohubId = Some("3"), isGoldenRecord = true),
        defaultContactPerson.copy(concatId = "AU~WUFOO~104", ohubId = Some("2"), isGoldenRecord = true)
      ).toDataset

      val previousMergedDs = Seq(
        defaultContactPerson.copy(ohubId = Some("1"), concatId = "AU~OHUB~1", isGoldenRecord = true),
        defaultContactPerson.copy(ohubId = Some("2"), concatId = "AU~OHUB~2", isGoldenRecord = true)
      ).toDataset

      val mergedDs = Seq(
        defaultContactPerson.copy(ohubId = Some("1"), concatId = "AU~OHUB~1", isGoldenRecord = true),
        defaultContactPerson.copy(ohubId = Some("2"), concatId = "AU~OHUB~2", isGoldenRecord = true),
        defaultContactPerson.copy(ohubId = Some("3"), concatId = "AU~OHUB~3", isGoldenRecord = true)
      ).toDataset

      SUT.export(integratedDs, getDeletedOhubIdsWithTargetId(spark, prevIntegratedDs, integratedDs, previousMergedDs, mergedDs).unionByName, mergedDs, previousMergedDs, config, spark)

      val result: Dataset[Row] = storage.readFromCsv(config.outboundLocation, new AcmOptions {}.delimiter, true)

      result.collect().length shouldBe 1
      result.filter($"DELETE_FLAG" === "0").collect().length shouldBe 1
      result.filter($"DELETE_FLAG" === "0").select("CP_ORIG_INTEGRATION_ID").collect().mkString(":") should include("[3]")
    }

    it("should export only golden records and when OhubId has changed in new integrated mark it as deleted with targetOhubId") {

      val prevIntegratedDs = Seq(
        defaultContactPerson.copy(concatId = "AU~WUFOO~101", ohubId = Some("1"), isGoldenRecord = true),
        defaultContactPerson.copy(concatId = "AU~WUFOO~102", ohubId = Some("1")),
        defaultContactPerson.copy(concatId = "AU~WUFOO~103", ohubId = Some("2"), isGoldenRecord = true),
        defaultContactPerson.copy(concatId = "AU~WUFOO~104", ohubId = Some("2")),
        defaultContactPerson.copy(concatId = "AU~WUFOO~901", ohubId = Some("5"), isGoldenRecord = true, isActive = true),
        defaultContactPerson.copy(concatId = "AU~WUFOO~902", ohubId = Some("5"), isActive = true)
      ).toDataset

      val integratedDs = Seq(
        defaultContactPerson.copy(concatId = "AU~WUFOO~101", ohubId = Some("1"), isGoldenRecord = true),
        defaultContactPerson.copy(concatId = "AU~WUFOO~102", ohubId = Some("3")),
        defaultContactPerson.copy(concatId = "AU~WUFOO~103", ohubId = Some("3")),
        defaultContactPerson.copy(concatId = "AU~WUFOO~104", ohubId = Some("3"), isGoldenRecord = true),
        defaultContactPerson.copy(concatId = "AU~WUFOO~901", ohubId = Some("5"), isGoldenRecord = true, isActive = false),
        defaultContactPerson.copy(concatId = "AU~WUFOO~902", ohubId = Some("5"), isActive = false)
      ).toDataset

      val previousMergedDs = Seq(
        defaultContactPerson.copy(ohubId = Some("1"), concatId = "AU~OHUB~1", isGoldenRecord = true),
        defaultContactPerson.copy(ohubId = Some("2"), concatId = "AU~OHUB~2", isGoldenRecord = true),
        defaultContactPerson.copy(ohubId = Some("5"), concatId = "AU~OHUB~5", isGoldenRecord = true)
      ).toDataset

      val mergedDs = Seq(
        defaultContactPerson.copy(ohubId = Some("1"), concatId = "AU~OHUB~1", isGoldenRecord = true),
        defaultContactPerson.copy(ohubId = Some("3"), concatId = "AU~OHUB~3", isGoldenRecord = true)
      ).toDataset

      SUT.export(integratedDs, getDeletedOhubIdsWithTargetId(spark,  prevIntegratedDs, integratedDs, previousMergedDs, mergedDs).unionByName, mergedDs, previousMergedDs, config, spark)

      val result = storage.readFromCsv(config.outboundLocation, new AcmOptions {}.delimiter, true)

      result.collect().length shouldBe 3
      result.filter($"DELETE_FLAG" === "0").collect().length shouldBe 1
      result.filter($"DELETE_FLAG" === "0").select("CP_ORIG_INTEGRATION_ID").collect().head.toString() should include("3")
      result.filter($"DELETE_FLAG" === "1").collect().length shouldBe 2

      /* We hav e one inconsintency, for deleted records and their concatid.
       * Because we look at the integrated for determining the deleted records and target groups, the concatid also comes from the integrated
       */
      result.filter($"CP_ORIG_INTEGRATION_ID" === "2").select("TARGET_OHUB_ID", "CP_LNKD_INTEGRATION_ID").collect().head.mkString(":") should include("3:AU~103~3~19")
      result.filter($"CP_ORIG_INTEGRATION_ID" === "5").select("TARGET_OHUB_ID").first.getString(0) should equal(null)
      result.filter($"CP_ORIG_INTEGRATION_ID" === "5").select("DELETE_FLAG").first.getString(0) should equal("1")
    }

    it("should export only golden records and when all ohubId has changed in integrated , then delete ones should be sent") {

      val prevIntegratedDs = Seq(
        defaultContactPerson.copy(concatId = "AU~WUFOO~101", ohubId = Some("1"), isGoldenRecord = true),
        defaultContactPerson.copy(concatId = "AU~WUFOO~102", ohubId = Some("1")),
        defaultContactPerson.copy(concatId = "AU~WUFOO~103", ohubId = Some("2"), isGoldenRecord = true),
        defaultContactPerson.copy(concatId = "AU~WUFOO~104", ohubId = Some("2"))
      ).toDataset

      val integratedDs = Seq(
        defaultContactPerson.copy(concatId = "AU~WUFOO~101", ohubId = Some("3")),
        defaultContactPerson.copy(concatId = "AU~WUFOO~102", ohubId = Some("3")),
        defaultContactPerson.copy(concatId = "AU~WUFOO~103", ohubId = Some("3")),
        defaultContactPerson.copy(concatId = "AU~WUFOO~104", ohubId = Some("3"), isGoldenRecord = true)
      ).toDataset

      val previousMergedDs = Seq(
        defaultContactPerson.copy(ohubId = Some("1"), concatId = "AU~OHUB~1", isGoldenRecord = true),
        defaultContactPerson.copy(ohubId = Some("2"), concatId = "AU~OHUB~2", isGoldenRecord = true)
      ).toDataset

      val mergedDs = Seq(
        defaultContactPerson.copy(ohubId = Some("3"), concatId = "AU~OHUB~3", isGoldenRecord = true)
      ).toDataset

      SUT.export(integratedDs, getDeletedOhubIdsWithTargetId(spark, prevIntegratedDs, integratedDs, previousMergedDs, mergedDs).unionByName, mergedDs, previousMergedDs, config, spark)

      val result: Dataset[Row] = storage.readFromCsv(config.outboundLocation, new AcmOptions {}.delimiter, true)

      result.collect().length shouldBe 3
      result.filter($"DELETE_FLAG" === "0").collect().length shouldBe 1
      result.filter($"DELETE_FLAG" === "1").collect().length shouldBe 2
      result.filter($"DELETE_FLAG" === "0").select("CP_ORIG_INTEGRATION_ID").collect().head.toString() should include("3")

      /* We have one inconsistency for deleted records and their concatid.
       * Because we look at the integrated for determining the deleted records and target groups, the concatid also comes from the integrated files
       */
      result.filter($"CP_ORIG_INTEGRATION_ID" === "2").select("TARGET_OHUB_ID", "CP_LNKD_INTEGRATION_ID", "DELETE_FLAG").collect().head.mkString(":") should include("3:AU~103~3~19:1")
      result.filter($"CP_ORIG_INTEGRATION_ID" === "1").select("TARGET_OHUB_ID", "CP_LNKD_INTEGRATION_ID", "DELETE_FLAG").collect().head.mkString(":") should include("3:AU~101~3~19:1")
    }

    it("should export new incoming merged ohubids to ACM only with emailAddress or phoneNumber" +
      " and set delete=true when email is invalid and mobile number is empty" ) {

      val prevIntegratedDS = Seq(
        defaultContactPerson.copy(isGoldenRecord = true, ohubId = Some("5555"), concatId = "AU~WUFOO~2345"),
        defaultContactPerson.copy(isGoldenRecord = true, ohubId = Some("5555"), concatId = "AU~WUFOO~0000")
      ).toDataset

      val integratedDs = Seq(
        defaultContactPerson.copy(isGoldenRecord = true, ohubId = Some("3211"), concatId = "AU~WUFOO~1234"),
        defaultContactPerson.copy(isGoldenRecord = true, ohubId = Some("5555"), concatId = "AU~WUFOO~2345"),
        defaultContactPerson.copy(isGoldenRecord = true, ohubId = Some("5555"), concatId = "AU~WUFOO~0000"),
        defaultContactPerson.copy(isGoldenRecord = true, ohubId = Some("8888"), concatId = "AU~WUFOO~4321",
          firstName = Some("New incoming Record with email and phone"), emailAddress = Some("x@ohub.com"), mobileNumber = Some("090909")),
        defaultContactPerson.copy(isGoldenRecord = true, ohubId = Some("9999"), concatId = "AU~WUFOO~1919",
          firstName = Some("New incoming Record with mobile only"), emailAddress = Some(""), mobileNumber = Some("090909")),
        defaultContactPerson.copy(isGoldenRecord = true, ohubId = Some("7777"), concatId = "AU~WUFOO~1717",
          firstName = Some("New incoming Record with email only"), emailAddress = Some("x@ohub.com"), mobileNumber = None),
        defaultContactPerson.copy(isGoldenRecord = true, ohubId = Some("6666"), concatId = "AU~WUFOO~1616",
          firstName = Some("New incoming Record without email nor phone"), emailAddress = Some(""), mobileNumber = None)
      ).toDataset

      val previousMergedDs = Seq(
        defaultContactPerson.copy(isGoldenRecord = true, ohubId = Some("3211"), concatId = "AU~OHUB~3211"),
        defaultContactPerson.copy(isGoldenRecord = true, ohubId = Some("5555"), concatId = "AU~OHUB~5555")
      ).toDataset

      val mergedDs = Seq(
        defaultContactPerson.copy(isGoldenRecord = true, ohubId = Some("3211"), concatId = "AU~OHUB~3211"),
        defaultContactPerson.copy(isGoldenRecord = true, ohubId = Some("5555"), concatId = "AU~OHUB~5555"),
        defaultContactPerson.copy(isGoldenRecord = true, ohubId = Some("8888"), concatId = "AU~OHUB~8888",
          emailAddress = Some("x@ohub.com"), mobileNumber = Some("090909"), isEmailAddressValid = Some(true)),
        defaultContactPerson.copy(isGoldenRecord = true, ohubId = Some("9999"), concatId = "AU~OHUB~9999",
          emailAddress = Some(""), mobileNumber = Some("090909")),
        defaultContactPerson.copy(isGoldenRecord = true, ohubId = Some("7777"), concatId = "AU~OHUB~7777",
          emailAddress = Some("x@ohub.com"), mobileNumber = None, isEmailAddressValid = Some(true)),
        defaultContactPerson.copy(isGoldenRecord = true, ohubId = Some("6666"), concatId = "AU~OHUB~6666",
          emailAddress = Some(""), mobileNumber = None),

        // This we discard (send as delete) because email is invalid and no mobile is available
        defaultContactPerson.copy(isGoldenRecord = true, ohubId = Some("4444"), concatId = "AU~OHUB~4444",
          emailAddress = Some("inv1@ohub.com"), mobileNumber = None, isEmailAddressValid = Some(false)),

        // This we keep because is reachable by SMS even though emailAddress is invalid
        defaultContactPerson.copy(isGoldenRecord = true, ohubId = Some("3333"), concatId = "AU~OHUB~3333",
          emailAddress = Some("inv2@ohub.com"), mobileNumber = Some("+31612345"), isEmailAddressValid = Some(false))
      ).toDataset

      SUT.export(integratedDs, getDeletedOhubIdsWithTargetId(spark, prevIntegratedDS, integratedDs, previousMergedDs, mergedDs).unionByName, mergedDs, previousMergedDs, config, spark)

      val result: Dataset[Row] = storage.readFromCsv(config.outboundLocation, new AcmOptions {}.delimiter, true)

      result.collect().length shouldBe 5
      result.filter($"CP_ORIG_INTEGRATION_ID" === "8888").select($"CP_LNKD_INTEGRATION_ID").collect().head.toString() should include("AU~8888~3~183")
      result.filter($"CP_ORIG_INTEGRATION_ID" === "9999").select($"CP_LNKD_INTEGRATION_ID").collect().head.toString() should include("AU~9999~3~183")
      result.filter($"CP_ORIG_INTEGRATION_ID" === "7777").select($"CP_LNKD_INTEGRATION_ID").collect().head.toString() should include("AU~7777~3~183")
      result.filter($"CP_ORIG_INTEGRATION_ID" === "3333").select($"DELETE_FLAG").first.getString(0) should equal("0")
      result.filter($"CP_ORIG_INTEGRATION_ID" === "3333").select($"TARGET_OHUB_ID").first.getString(0) should equal(null)
      result.filter($"CP_ORIG_INTEGRATION_ID" === "3333").select($"EMAIL_ADDRESS").first.getString(0) should equal("inv2@ohub.com")
      result.filter($"CP_ORIG_INTEGRATION_ID" === "3333").select($"MOBILE_PHONE_NUMBER").first.getString(0) should equal("+31612345")
      result.filter($"CP_ORIG_INTEGRATION_ID" === "4444").select($"DELETE_FLAG").first.getString(0) should equal("1")
      result.filter($"CP_ORIG_INTEGRATION_ID" === "4444").select($"EMAIL_ADDRESS").first.getString(0) should equal("inv1@ohub.com")
    }

    it("should send DE-ACTIVATED groups as deleted") {

      val prevIntegratedDs = Seq(
        defaultContactPerson.copy(concatId = "AU~WUFOO~101", ohubId = Some("1"), isGoldenRecord = true, isActive = true),
        defaultContactPerson.copy(concatId = "AU~WUFOO~102", ohubId = Some("1"), isActive = true),
        defaultContactPerson.copy(concatId = "AU~WUFOO~104", ohubId = Some("2"), isGoldenRecord = true, isActive = true),
        defaultContactPerson.copy(concatId = "AU~WUFOO~105", ohubId = Some("2"), isGoldenRecord = true, isActive = true)
      ).toDataset

      val integratedDs = Seq(
        defaultContactPerson.copy(concatId = "AU~WUFOO~101", ohubId = Some("1"), isGoldenRecord = true, isActive = false),
        defaultContactPerson.copy(concatId = "AU~WUFOO~102", ohubId = Some("1"), isActive = false),
        defaultContactPerson.copy(concatId = "AU~WUFOO~104", ohubId = Some("2"), isGoldenRecord = true, isActive = true),
        defaultContactPerson.copy(concatId = "AU~WUFOO~105", ohubId = Some("2"), isGoldenRecord = false, isActive = false)
      ).toDataset

      val previousMergedDs = Seq(
        defaultContactPerson.copy(ohubId = Some("1"), concatId = "AU~OHUB~1", isGoldenRecord = true, isActive = true),
        defaultContactPerson.copy(ohubId = Some("2"), concatId = "AU~OHUB~2", isGoldenRecord = true, isActive = true)
      ).toDataset

      val mergedDs = Seq(
        defaultContactPerson.copy(ohubId = Some("2"), concatId = "AU~OHUB~2", isGoldenRecord = true, isActive = true)
      ).toDataset

      SUT.export(integratedDs, getDeletedOhubIdsWithTargetId(spark, prevIntegratedDs, integratedDs, previousMergedDs, mergedDs).unionByName, mergedDs, previousMergedDs, config, spark)

      val result: Dataset[Row] = storage.readFromCsv(config.outboundLocation, new AcmOptions {}.delimiter, true)

      result.collect().length shouldBe 1
      result.filter($"DELETE_FLAG" === "0").collect().length shouldBe 0
      result.filter($"DELETE_FLAG" === "1").collect().length shouldBe 1
      result.filter($"CP_ORIG_INTEGRATION_ID" === "1").select("TARGET_OHUB_ID").first.getString(0) should equal(null)
      result.filter($"CP_ORIG_INTEGRATION_ID" === "1").select("DELETE_FLAG").first.getString(0) should equal("1")
    }

    it("should send changed groups and DE-ACTIVATED without interfering with each other") {

      val prevIntegratedDs = Seq(
        defaultContactPerson.copy(concatId = "AU~WUFOO~101", ohubId = Some("1"), isGoldenRecord = true),
        defaultContactPerson.copy(concatId = "AU~WUFOO~102", ohubId = Some("1")),
        defaultContactPerson.copy(concatId = "AU~WUFOO~103", ohubId = Some("2"), isGoldenRecord = true),
        defaultContactPerson.copy(concatId = "AU~WUFOO~104", ohubId = Some("2")),
        defaultContactPerson.copy(concatId = "AU~WUFOO~901", ohubId = Some("5"), isGoldenRecord = true, isActive = true),
        defaultContactPerson.copy(concatId = "AU~WUFOO~902", ohubId = Some("5"), isActive = true),
        defaultContactPerson.copy(concatId = "AU~WUFOO~906", ohubId = Some("6"), firstName=Some("Rob"), isActive = true)
      ).toDataset

      val integratedDs = Seq(
        defaultContactPerson.copy(concatId = "AU~WUFOO~101", ohubId = Some("3")),
        defaultContactPerson.copy(concatId = "AU~WUFOO~102", ohubId = Some("3")),
        defaultContactPerson.copy(concatId = "AU~WUFOO~103", ohubId = Some("3")),
        defaultContactPerson.copy(concatId = "AU~WUFOO~104", ohubId = Some("3"), isGoldenRecord = true),
        defaultContactPerson.copy(concatId = "AU~WUFOO~901", ohubId = Some("5"), isGoldenRecord = true, isActive = false),
        defaultContactPerson.copy(concatId = "AU~WUFOO~902", ohubId = Some("5"), isActive = false),
        defaultContactPerson.copy(concatId = "AU~WUFOO~906", ohubId = Some("6"), firstName=Some("Matthew"), isActive = true)
      ).toDataset

      val previousMergedDs = Seq(
        defaultContactPerson.copy(ohubId = Some("1"), concatId = "AU~OHUB~1", isGoldenRecord = true),
        defaultContactPerson.copy(ohubId = Some("2"), concatId = "AU~OHUB~2", isGoldenRecord = true),
        defaultContactPerson.copy(ohubId = Some("5"), concatId = "AU~OHUB~5", isGoldenRecord = true),
        defaultContactPerson.copy(ohubId = Some("6"), concatId = "AU~OHUB~6", isGoldenRecord = true, firstName=Some("Rob"))
      ).toDataset

      val mergedDs = Seq(
        defaultContactPerson.copy(ohubId = Some("3"), concatId = "AU~OHUB~3", isGoldenRecord = true),
        defaultContactPerson.copy(ohubId = Some("6"), concatId = "AU~OHUB~6", isGoldenRecord = true, firstName=Some("Matthew"))
      ).toDataset

      SUT.export(integratedDs, getDeletedOhubIdsWithTargetId(spark, prevIntegratedDs, integratedDs, previousMergedDs, mergedDs).unionByName, mergedDs, previousMergedDs, config, spark)

      val result: Dataset[Row] = storage.readFromCsv(config.outboundLocation, new AcmOptions {}.delimiter, true)

      result.collect().length shouldBe 5
      result.filter($"DELETE_FLAG" === "0").collect().length shouldBe 2
      result.filter($"DELETE_FLAG" === "1").collect().length shouldBe 3

      // Delete flags
      result.filter($"CP_ORIG_INTEGRATION_ID" === "5").select("DELETE_FLAG").first.getString(0) should equal("1")
      result.filter($"CP_ORIG_INTEGRATION_ID" === "5").select("TARGET_OHUB_ID").first.getString(0) should equal(null)
      result.filter($"CP_ORIG_INTEGRATION_ID" === "1").select("DELETE_FLAG").first.getString(0) should equal("1")
      result.filter($"CP_ORIG_INTEGRATION_ID" === "2").select("DELETE_FLAG").first.getString(0) should equal("1")
      result.filter($"CP_ORIG_INTEGRATION_ID" === "3").select("DELETE_FLAG").first.getString(0) should equal("0")
      result.filter($"CP_ORIG_INTEGRATION_ID" === "6").select("DELETE_FLAG").first.getString(0) should equal("0")

      result.filter($"CP_ORIG_INTEGRATION_ID" === "2").select("TARGET_OHUB_ID", "CP_LNKD_INTEGRATION_ID", "DELETE_FLAG").collect().head.mkString(":") should include("3:AU~103~3~19:1")
      result.filter($"CP_ORIG_INTEGRATION_ID" === "1").select("TARGET_OHUB_ID", "CP_LNKD_INTEGRATION_ID", "DELETE_FLAG").collect().head.mkString(":") should include("3:AU~101~3~19:1")

    }
  }

  def readFirstLine(path: Path, fs: FileSystem) = {
    val reader = new BufferedReader(new InputStreamReader(fs.open(path), "UTF-8"))
    reader.readLine()
  }
}
