package com.unilever.ohub.spark.export.acm

import java.io.{BufferedReader, File, InputStreamReader}
import java.util.UUID

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.domain.entity.TestContactPersons
import com.unilever.ohub.spark.export.TargetType
import com.unilever.ohub.spark.export.domain.InMemStorage
import com.unilever.ohub.spark.{SparkJobSpec, export}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.{BeforeAndAfter, Matchers}

class AcmContactPersonsExportWriterSpec extends SparkJobSpec with TestContactPersons with BeforeAndAfter with Matchers {

  import spark.implicits._

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
  val storage = new InMemStorage(spark, contactPersons, prevIntegrated)

  after {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.delete(new Path(config.outboundLocation), true)
  }

  describe("ACM csv generation") {
    it("Should write correct csv") {
      SUT.export(contactPersons, prevIntegrated, config, spark)

      val result = storage.readFromCsv(config.outboundLocation, new AcmOptions {}.delimiter, true)

      result.collect().length shouldBe 1
      result.collect().head.toString() should include("AU~1234~3~19")
    }

    it("Should fitler based on coutryCodes") {
      val configWithCountries = config.copy(countryCodes = Some(Seq("NL")))

      SUT.export(contactPersons, prevIntegrated, configWithCountries, spark)

      val result = storage.readFromCsv(config.outboundLocation, new AcmOptions {}.delimiter, true)

      result.collect().length shouldBe 0
    }

    it("Should contain header") {
      SUT.export(contactPersons, prevIntegrated, config, spark)

      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val csvFile = fs.listStatus(new Path(config.outboundLocation)).find(status => status.getPath.getName.contains("UFS_RECIPIENTS")).get
      val header = readFirstLine(csvFile.getPath, fs)
      header shouldBe "CP_ORIG_INTEGRATION_ID¶CP_LNKD_INTEGRATION_ID¶OPR_ORIG_INTEGRATION_ID¶GOLDEN_RECORD_FLAG¶WEB_CONTACT_ID¶EMAIL_OPTOUT¶PHONE_OPTOUT¶FAX_OPTOUT¶MOBILE_OPTOUT¶DM_OPTOUT¶LAST_NAME¶FIRST_NAME¶MIDDLE_NAME¶TITLE¶GENDER¶LANGUAGE¶EMAIL_ADDRESS¶MOBILE_PHONE_NUMBER¶PHONE_NUMBER¶FAX_NUMBER¶STREET¶HOUSENUMBER¶ZIPCODE¶CITY¶COUNTRY¶DATE_CREATED¶DATE_UPDATED¶DATE_OF_BIRTH¶PREFERRED¶ROLE¶COUNTRY_CODE¶SCM¶DELETE_FLAG¶KEY_DECISION_MAKER¶OPT_IN¶OPT_IN_DATE¶CONFIRMED_OPT_IN¶CONFIRMED_OPT_IN_DATE¶MOB_OPT_IN¶MOB_OPT_IN_DATE¶MOB_CONFIRMED_OPT_IN¶MOB_CONFIRMED_OPT_IN_DATE¶MOB_OPT_OUT_DATE¶ORG_FIRST_NAME¶ORG_LAST_NAME¶ORG_EMAIL_ADDRESS¶ORG_FIXED_PHONE_NUMBER¶ORG_MOBILE_PHONE_NUMBER¶ORG_FAX_NUMBER¶HAS_REGISTRATION¶REGISTRATION_DATE¶HAS_CONFIRMED_REGISTRATION¶CONFIRMED_REGISTRATION_DATE¶TARGET_OHUB_ID"
    }

    it("Should write the conversionMapping in json format") {
      val mappingLocation = new Path(outboundLocation, "contactperson-mapping.json")
      SUT.export(contactPersons, prevIntegrated, config.copy(mappingOutputLocation = Some(mappingLocation.toString)), spark)

      val mapper = new ObjectMapper() with ScalaObjectMapper
      mapper.registerModule(DefaultScalaModule)

      val conversionMapping = mapper.readTree(new File(mappingLocation.toString))
      conversionMapping.size() > 50 shouldBe true
      conversionMapping.get("REGISTRATION_DATE").size() shouldBe 8
    }

    it("should export deleted ohubId along with changed contactperson to ACM") {
      val deletedContactPerson = defaultContactPerson.copy(ohubId = Some("9876"), concatId = "AU~WUFOO~1234", isGoldenRecord = true)
      val integratedDs = contactPersons
      val prevIntegratedDS = Seq(unchangedCP, defaultContactPerson, deletedContactPerson).toDataset

      SUT.export(integratedDs, prevIntegratedDS, config, spark)

      val result: Dataset[Row] = storage.readFromCsv(config.outboundLocation, new AcmOptions {}.delimiter, true)

      result.collect().length shouldBe 2
      result.filter($"CP_ORIG_INTEGRATION_ID" === "9876").select($"TARGET_OHUB_ID").collect().head.toString() should include("1234")
    }

    it("should export only golden records and changed one") {
      val integratedDs = Seq(
        defaultContactPerson.copy(concatId = "AU~WUFOO~101", ohubId = Some("1"), isGoldenRecord = true),
        defaultContactPerson.copy(concatId = "AU~WUFOO~102", ohubId = Some("3")),
        defaultContactPerson.copy(concatId = "AU~WUFOO~103", ohubId = Some("3"), isGoldenRecord = true),
        defaultContactPerson.copy(concatId = "AU~WUFOO~104", ohubId = Some("2"), isGoldenRecord = true)
      ).toDataset

      val prevIntegratedDs = Seq(
        defaultContactPerson.copy(concatId = "AU~WUFOO~101", ohubId = Some("1"), isGoldenRecord = true),
        defaultContactPerson.copy(concatId = "AU~WUFOO~102", ohubId = Some("1")),
        defaultContactPerson.copy(concatId = "AU~WUFOO~103", ohubId = Some("2"), isGoldenRecord = true),
        defaultContactPerson.copy(concatId = "AU~WUFOO~104", ohubId = Some("2"))
      ).toDataset

      SUT.export(integratedDs, prevIntegratedDs, config, spark)

      val result: Dataset[Row] = storage.readFromCsv(config.outboundLocation, new AcmOptions {}.delimiter, true)

      result.collect().length shouldBe 2
      result.filter($"DELETE_FLAG" === "0").collect().length shouldBe 2
      result.filter($"DELETE_FLAG" === "0").select("CP_ORIG_INTEGRATION_ID").collect().mkString(":") should include ("[3]:[2]")
    }

    it("should export only golden records and when OhubId has changed in new integrated mark it as deleted with targetOhubId") {
      val integratedDs = Seq(
        defaultContactPerson.copy(concatId = "AU~WUFOO~101", ohubId = Some("1"), isGoldenRecord = true),
        defaultContactPerson.copy(concatId = "AU~WUFOO~102", ohubId = Some("3")),
        defaultContactPerson.copy(concatId = "AU~WUFOO~103", ohubId = Some("3")),
        defaultContactPerson.copy(concatId = "AU~WUFOO~104", ohubId = Some("3"), isGoldenRecord = true)
      ).toDataset

      val prevIntegratedDs = Seq(
        defaultContactPerson.copy(concatId = "AU~WUFOO~101", ohubId = Some("1"), isGoldenRecord = true),
        defaultContactPerson.copy(concatId = "AU~WUFOO~102", ohubId = Some("1")),
        defaultContactPerson.copy(concatId = "AU~WUFOO~103", ohubId = Some("2"), isGoldenRecord = true),
        defaultContactPerson.copy(concatId = "AU~WUFOO~104", ohubId = Some("2"))
      ).toDataset

      SUT.export(integratedDs, prevIntegratedDs, config, spark)

      val result = storage.readFromCsv(config.outboundLocation, new AcmOptions {}.delimiter, true)

      result.collect().length shouldBe 2
      result.filter($"DELETE_FLAG" === "0").collect() should have length 1
      result.filter($"DELETE_FLAG" === "0").select("CP_LNKD_INTEGRATION_ID").collect().head.toString() should include("AU~104~3~19")
      result.filter($"DELETE_FLAG" === "1").select("TARGET_OHUB_ID","CP_LNKD_INTEGRATION_ID").collect().head.mkString(":") should include("3:AU~103~3~19")

    }

    it("should export only golden records and when all ohubId has changed in integrated , then delete ones should be sent") {

      val integratedDs = Seq(
        defaultContactPerson.copy(concatId = "AU~WUFOO~101", ohubId = Some("3")),
        defaultContactPerson.copy(concatId = "AU~WUFOO~102", ohubId = Some("3")),
        defaultContactPerson.copy(concatId = "AU~WUFOO~103", ohubId = Some("3")),
        defaultContactPerson.copy(concatId = "AU~WUFOO~104", ohubId = Some("3"), isGoldenRecord = true)
      ).toDataset

      val prevIntegratedDs = Seq(
        defaultContactPerson.copy(concatId = "AU~WUFOO~101", ohubId = Some("1"), isGoldenRecord = true),
        defaultContactPerson.copy(concatId = "AU~WUFOO~102", ohubId = Some("1")),
        defaultContactPerson.copy(concatId = "AU~WUFOO~103", ohubId = Some("2"), isGoldenRecord = true),
        defaultContactPerson.copy(concatId = "AU~WUFOO~104", ohubId = Some("2"))
      ).toDataset

      SUT.export(integratedDs, prevIntegratedDs, config, spark)

      val result: Dataset[Row] = storage.readFromCsv(config.outboundLocation, new AcmOptions {}.delimiter, true)

      result.collect().length shouldBe 3
      result.filter($"DELETE_FLAG" === "0").collect().length shouldBe 1
      result.filter($"DELETE_FLAG" === "1").collect().length shouldBe 2

      result.filter($"DELETE_FLAG" === "0").select("CP_LNKD_INTEGRATION_ID").collect().head.toString() should include("AU~104~3~19")
      result.filter($"CP_ORIG_INTEGRATION_ID" === "2").select("TARGET_OHUB_ID","CP_LNKD_INTEGRATION_ID", "DELETE_FLAG").collect().head.mkString(":") should include("3:AU~103~3~19:1")
      result.filter($"CP_ORIG_INTEGRATION_ID" === "1").select("TARGET_OHUB_ID","CP_LNKD_INTEGRATION_ID", "DELETE_FLAG").collect().head.mkString(":") should include("3:AU~101~3~19:1")
    }
}

  def readFirstLine(path: Path, fs: FileSystem) = {
    val reader = new BufferedReader(new InputStreamReader(fs.open(path), "UTF-8"))
    reader.readLine()
  }
}
