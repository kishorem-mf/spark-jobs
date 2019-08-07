package com.unilever.ohub.spark.export.domain

import java.util.UUID

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.domain.DomainEntityHash
import com.unilever.ohub.spark.domain.entity.{ContactPerson, TestContactPersons}
import com.unilever.ohub.spark.export.TargetType.{ACM, DATASCIENCE, MEPS}
import com.unilever.ohub.spark.outbound.InMemStorage
import com.unilever.ohub.spark.{SparkJobSpec, export}

class ContactPersonExportWriterSpecs extends SparkJobSpec with TestContactPersons {
  import spark.implicits._

  private val changedCP = defaultContactPerson.copy(isGoldenRecord = true)
  private val unchangedCP = defaultContactPerson.copy(isGoldenRecord = true, concatId = "A~B~C")
  private val contactPersons = Seq(changedCP, unchangedCP, defaultContactPerson).toDataset
  private val hashes = Seq[DomainEntityHash](DomainEntityHash(changedCP.concatId, Some(true), Some("some-hash")), DomainEntityHash(unchangedCP.concatId, Some(false), Some("hash"))).toDataset

  val SUT = ContactPerson.domainExportWriter.get

  val storage = new InMemStorage(spark, contactPersons, hashes)

  private val config = export.OutboundConfig(
    integratedInputFile = "integratedFolder",
    outboundLocation = UUID.randomUUID().toString,
    targetType = MEPS
  )

//  after {
//    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
//    fs.delete(new Path(config.outboundLocation), true)
//  }

  describe("ContactPersonExportWriter") {
    describe("#entityName") {
      it("should have the correct Entity name") {
        SUT.entityName() shouldBe ContactPerson.engineFolderName
      }
    }

    describe("#mergeCsvFiles") {
      it("should/shouldn't merge based on targetType (otherwise it can be a value/removed as a whole)") {
        SUT.mergeCsvFiles(DATASCIENCE) should not be SUT.mergeCsvFiles(MEPS)
      }

      it("shouldn't allow unimplemented targetTypes") {
        assertThrows[IllegalArgumentException] {
          SUT.mergeCsvFiles(ACM)
        }
      }
    }

    describe("#filename") {
      it("should/shouldn't return the same filesnames based on targetType (otherwise it can be a value/removed as a whole)") {
        SUT.filename(DATASCIENCE) should not be SUT.filename(MEPS)
      }

      it("shouldn't allow unimplemented targetTypes") {
        assertThrows[IllegalArgumentException] {
          SUT.filename(ACM)
        }
      }
    }

//    describe("#run") {
//      it("should write to a single file for ARMSTRONG") {
//        SUT.export(contactPersons, hashes, config.copy(targetType = MEPS), spark)
//
//        val result = storage.readFromCsv(config.outboundLocation, new AcmOptions {}.delimiter, true)
//
//        result.collect().length shouldBe 1
//      }
//    }
  }
}
