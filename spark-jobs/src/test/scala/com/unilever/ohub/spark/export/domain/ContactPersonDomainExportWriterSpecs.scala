package com.unilever.ohub.spark.export.domain

import java.util.UUID

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.domain.DomainEntityHash
import com.unilever.ohub.spark.domain.entity.{ContactPerson, TestContactPersons}
import com.unilever.ohub.spark.export.TargetType.{DATASCIENCE, MEPS}
import com.unilever.ohub.spark.outbound.InMemStorage
import com.unilever.ohub.spark.{SparkJobSpec, export}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.BeforeAndAfter

class ContactPersonDomainExportWriterSpecs extends SparkJobSpec with BeforeAndAfter with TestContactPersons {

  import spark.implicits._

  private val auCP1 = defaultContactPerson.copy(concatId = "AU~CP1", countryCode = "AU")
  private val pkCP1 = defaultContactPerson.copy(concatId = "PK~CP1", countryCode = "PK")
  private val pkCP2 = defaultContactPerson.copy(concatId = "PK~CP2", countryCode = "PK")
  private val contactPersons = Seq(pkCP1, pkCP2, auCP1).toDataset
  private val hashes = Seq[DomainEntityHash](DomainEntityHash(pkCP1.concatId, Some(true), Some("some-hash")), DomainEntityHash(pkCP2.concatId, Some(false), Some("hash"))).toDataset

  val SUT = ContactPerson.domainExportWriter.get

  val storage = new InMemStorage(spark, contactPersons, hashes)

  private val config = export.OutboundConfig(
    integratedInputFile = "integratedFolder",
    outboundLocation = UUID.randomUUID().toString
  )

  after {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.delete(new Path(config.outboundLocation), true)
  }

  describe("ContactPersonDomainExportWriter") {
    describe("MEPS export") {
      it("Should filter based on allowed countryCodes") {
        val mepsConfig = config.copy(targetType = MEPS)

        SUT.export(contactPersons, hashes, mepsConfig, spark)
        val result = storage.readFromCsv(mepsConfig.outboundLocation, new DomainExportOptions {}.delimiter, true).orderBy($"concatId".asc).collect()

        result.length shouldBe 2
        result(0).getString(result(0).fieldIndex("concatId")) shouldBe pkCP1.concatId
        result(1).getString(result(1).fieldIndex("concatId")) shouldBe pkCP2.concatId
      }
    }

    describe("DATSCIENCE export") {
      it("Should output all CP's and skip merging (also use a different output folder)") {
        val dbConfig = config.copy(targetType = DATASCIENCE)

        SUT.export(contactPersons, hashes, dbConfig, spark)
        val result = storage.readFromCsv(s"${dbConfig.outboundLocation}/${ContactPerson.engineFolderName}/datascience", new DomainExportOptions {}.delimiter, true).orderBy($"concatId".asc).collect()

        result.length shouldBe 3
        result(0).getString(result(0).fieldIndex("concatId")) shouldBe auCP1.concatId
        result(1).getString(result(1).fieldIndex("concatId")) shouldBe pkCP1.concatId
        result(2).getString(result(2).fieldIndex("concatId")) shouldBe pkCP2.concatId
      }
    }
  }
}
