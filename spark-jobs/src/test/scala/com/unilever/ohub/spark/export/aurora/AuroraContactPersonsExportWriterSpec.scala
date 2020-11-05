package com.unilever.ohub.spark.export.aurora

import java.sql.Timestamp
import java.util.UUID

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.domain.entity.{ContactPerson, TestContactPersons}
import com.unilever.ohub.spark.export.TargetType
import com.unilever.ohub.spark.export.domain.InMemStorage
import com.unilever.ohub.spark.{SparkJobSpec, export}
import org.apache.spark.sql.{Dataset, Row}

class AuroraContactPersonsExportWriterSpec extends SparkJobSpec with TestContactPersons {

  import spark.implicits._

  private val SUT = com.unilever.ohub.spark.export.aurora.ContactPersonOutboundWriter
  private val outboundLocation = UUID.randomUUID().toString+"/"
  private val config = export.OutboundConfig(
    integratedInputFile = "integrated",
    outboundLocation = outboundLocation,
    targetType = TargetType.AURORA,
    auroraCountryCodes = "AT;DE"
  )


  describe("Aurora parquet generation") {

    val cp1 = defaultContactPerson.copy(firstName = Some("a"), ohubId = Some("G"),
      dateUpdated = None,
      dateCreated = Some(Timestamp.valueOf("2015-09-30 14:23:05.0")),
      countryCode = "AT")

    val cp2 = defaultContactPerson.copy(firstName = Some("b"), ohubId = Some("S"),
      dateUpdated = None,
      dateCreated = Some(Timestamp.valueOf("2015-09-30 14:23:05.0")),
      countryCode = "DE"
    )
    val cp3 = defaultContactPerson.copy(firstName = Some("c"), ohubId = Some("V"),
      dateUpdated = None,
      dateCreated = Some(Timestamp.valueOf("2015-09-30 14:23:05.0")),
      countryCode = "NL"
    )
    val cp4 = defaultContactPerson.copy(firstName = Some("d"), ohubId = Some("W"),
      dateUpdated = None,
      dateCreated = Some(Timestamp.valueOf("2015-09-30 14:23:05.0")),
      countryCode = "NL"
    )

    val contactPersons: Dataset[ContactPerson] = Seq(cp1, cp2, cp3).toDataset
    // As InMemStorage has prevIntegrated as third parameter which is mandatory, we are passing the same integrated there
    val storage = new InMemStorage(spark, contactPersons, contactPersons)

    val result = SUT.run(spark, config, storage)

    it("Should filter based on countryCodes") {

      val resultparquet = spark.read.parquet(config.outboundLocation + "*/contactpersons/Processed/contactpersons.parquet")
      resultparquet.select("countryCode").distinct().count() shouldBe 2

    }



    it("Should not contain non aurora countries") {

      val resultparquet = spark.read.parquet(config.outboundLocation + "*/contactpersons/Processed/contactpersons.parquet")
      resultparquet.filter($"countryCode" === "NL").select($"countryCode").distinct().count() shouldBe 0
    }
  }

}
