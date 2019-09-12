package com.unilever.ohub.spark.export.dispatch

import java.util.UUID

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.domain.entity.TestContactPersons
import com.unilever.ohub.spark.export.TargetType
import com.unilever.ohub.spark.export.domain.InMemStorage
import com.unilever.ohub.spark.{SparkJobSpec, export}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.BeforeAndAfter

class DispatcherContactPersonsExportOutboundWriterSpec extends SparkJobSpec with TestContactPersons with BeforeAndAfter {

  import spark.implicits._

  private val cp = defaultContactPerson.copy(isGoldenRecord = true)
  private val SUT = com.unilever.ohub.spark.export.dispatch.ContactPersonOutboundWriter
  private val contactPersons = Seq(cp, defaultContactPerson).toDataset
  private val prevInteg = Seq(cp.copy(isActive = false)).toDataset
  private val config = export.OutboundConfig(
    integratedInputFile = "integrated",
    outboundLocation = UUID.randomUUID().toString,
    targetType = TargetType.DISPATCHER
  )
  val storage = new InMemStorage(spark, contactPersons, prevInteg)

  after {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.delete(new Path(config.outboundLocation), true)
  }

  describe("DDB csv generation") {
    it("Should write correct csv") {
      SUT.export(contactPersons, prevInteg, config, spark)

      val result = storage.readFromCsv(config.outboundLocation, new DispatcherOptions {}.delimiter, true)
      assert(result.collect().length >= 1)
      assert(result.head().get(0).equals("AU"))
    }

    it("Should export golden and non golden records") {
      SUT.export(contactPersons, prevInteg, config, spark)

      val result = storage.readFromCsv(config.outboundLocation, new DispatcherOptions {}.delimiter, true)
      assert(result.collect().length == 2)
    }

    it("Should contain header with quotes") {
      SUT.export(contactPersons, prevInteg, config, spark)

      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val csvFile = fs.listStatus(new Path(config.outboundLocation)).find(status => status.getPath.getName.contains("UFS")).get
      val header = fs.open(csvFile.getPath).readLine()
      assert(header.equals("\"COUNTRY_CODE\";\"CP_ORIG_INTEGRATION_ID\";\"CP_LNKD_INTEGRATION_ID\";\"GOLDEN_RECORD_FLAG\";\"SOURCE\";\"SOURCE_ID\";\"DELETE_FLAG\";\"CREATED_AT\";\"UPDATED_AT\";\"GENDER\";\"ROLE\";\"TITLE\";\"FIRST_NAME\";\"MIDDLE_NAME\";\"LAST_NAME\";\"STREET\";\"HOUSE_NUMBER\";\"HOUSE_NUMBER_ADD\";\"ZIP_CODE\";\"CITY\";\"COUNTRY\";\"DM_OPT_OUT\";\"EMAIL_ADDRESS\";\"EMAIL_OPT_OUT\";\"FIXED_OPT_OUT\";\"FIXED_PHONE_NUMBER\";\"MOBILE_OPT_OUT\";\"MOBILE_PHONE_NUMBER\";\"LANGUAGE\";\"PREFERRED\";\"KEY_DECISION_MAKER\";\"FAX_OPT_OUT\";\"FAX_NUMBER\";\"DATE_OF_BIRTH\";\"SCM\";\"STATE\";\"OPT_IN\";\"OPT_IN_DATE\";\"CONFIRMED_OPT_IN\";\"CONFIRMED_OPT_IN_DATE\";\"OPR_ORIG_INTEGRATION_ID\";\"ORG_FIRST_NAME\";\"ORG_LAST_NAME\";\"ORG_EMAIL_ADDRESS\";\"ORG_FIXED_PHONE_NUMBER\";\"ORG_MOBILE_PHONE_NUMBER\";\"ORG_FAX_NUMBER\";\"MOB_OPT_IN\";\"MOB_OPT_IN_DATE\";\"MOB_CONFIRMED_OPT_IN\";\"MOB_CONFIRMED_OPT_IN_DATE\";\"MOB_OPT_OUT_DATE\""))
    }
  }
}
