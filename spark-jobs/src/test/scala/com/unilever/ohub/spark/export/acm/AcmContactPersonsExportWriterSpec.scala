package com.unilever.ohub.spark.export.acm

import java.io.{BufferedReader, File, InputStreamReader}
import java.util.UUID

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.domain.DomainEntityHash
import com.unilever.ohub.spark.domain.entity.TestContactPersons
import com.unilever.ohub.spark.export.TargetType
import com.unilever.ohub.spark.outbound.InMemStorage
import com.unilever.ohub.spark.{SparkJobSpec, export}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{BeforeAndAfter, Matchers}

class AcmContactPersonsExportWriterSpec extends SparkJobSpec with TestContactPersons with BeforeAndAfter with Matchers {

  import spark.implicits._

  private val changedCP = defaultContactPerson.copy(isGoldenRecord = true)
  private val unchangedCP = defaultContactPerson.copy(isGoldenRecord = true, concatId = "A~B~C")
  private val SUT = com.unilever.ohub.spark.export.acm.ContactPersonOutboundWriter
  private val contactPersons = Seq(changedCP, unchangedCP, defaultContactPerson).toDataset
  private val hashes = Seq[DomainEntityHash](DomainEntityHash(changedCP.concatId, Some(true), Some("some-hash")), DomainEntityHash(unchangedCP.concatId, Some(false), Some("hash"))).toDataset
  private val outboundLocation = UUID.randomUUID().toString
  private val config = export.OutboundConfig(
    integratedInputFile = "integrated",
    outboundLocation = outboundLocation,
    targetType = TargetType.ACM
  )
  val storage = new InMemStorage(spark, contactPersons, hashes)

  after {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.delete(new Path(config.outboundLocation), true)
  }

  describe("ACM csv generation") {
    it("Should write correct csv") {
      SUT.export(contactPersons, hashes, config, spark)

      val result = storage.readFromCsv(config.outboundLocation, new AcmOptions {}.delimiter, true)

      result.collect().length shouldBe 1
      result.collect().head.toString() should include("AU~AB123~3~19")
    }

    it("Should contain header") {
      SUT.export(contactPersons, hashes, config, spark)

      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val csvFile = fs.listStatus(new Path(config.outboundLocation)).find(status => status.getPath.getName.contains("UFS_RECIPIENTS")).get
      val header = readFirstLine(csvFile.getPath, fs)
      header shouldBe "CP_ORIG_INTEGRATION_ID¶CP_LNKD_INTEGRATION_ID¶OPR_ORIG_INTEGRATION_ID¶GOLDEN_RECORD_FLAG¶WEB_CONTACT_ID¶EMAIL_OPTOUT¶PHONE_OPTOUT¶FAX_OPTOUT¶MOBILE_OPTOUT¶DM_OPTOUT¶LAST_NAME¶FIRST_NAME¶MIDDLE_NAME¶TITLE¶GENDER¶LANGUAGE¶EMAIL_ADDRESS¶MOBILE_PHONE_NUMBER¶PHONE_NUMBER¶FAX_NUMBER¶STREET¶HOUSENUMBER¶ZIPCODE¶CITY¶COUNTRY¶DATE_CREATED¶DATE_UPDATED¶DATE_OF_BIRTH¶PREFERRED¶ROLE¶COUNTRY_CODE¶SCM¶DELETE_FLAG¶KEY_DECISION_MAKER¶OPT_IN¶OPT_IN_DATE¶CONFIRMED_OPT_IN¶CONFIRMED_OPT_IN_DATE¶MOB_OPT_IN¶MOB_OPT_IN_DATE¶MOB_CONFIRMED_OPT_IN¶MOB_CONFIRMED_OPT_IN_DATE¶MOB_OPT_OUT_DATE¶ORG_FIRST_NAME¶ORG_LAST_NAME¶ORG_EMAIL_ADDRESS¶ORG_FIXED_PHONE_NUMBER¶ORG_MOBILE_PHONE_NUMBER¶ORG_FAX_NUMBER¶HAS_REGISTRATION¶REGISTRATION_DATE¶HAS_CONFIRMED_REGISTRATION¶CONFIRMED_REGISTRATION_DATE"
    }

    it("Should write the conversionMapping in json format") {
      val mappingLocation = new Path(outboundLocation, "contactperson-mapping.json")
      SUT.export(contactPersons, hashes, config.copy(mappingOutputLocation = Some(mappingLocation.toString)), spark)

      val mapper = new ObjectMapper() with ScalaObjectMapper
      mapper.registerModule(DefaultScalaModule)

      val conversionMapping = mapper.readTree(new File(mappingLocation.toString))
      conversionMapping.size() > 50 shouldBe true
      conversionMapping.get("REGISTRATION_DATE").size() shouldBe 8
    }
  }

  def readFirstLine(path: Path, fs: FileSystem) = {
    val reader = new BufferedReader(new InputStreamReader(fs.open(path), "UTF-8"))
    reader.readLine()
  }
}
