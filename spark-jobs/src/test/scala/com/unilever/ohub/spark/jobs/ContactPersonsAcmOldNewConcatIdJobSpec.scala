package com.unilever.ohub.spark.jobs

import java.util.UUID

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.DomainEntityHash
import com.unilever.ohub.spark.domain.entity.{TestContactPersons, TestOperators}
import com.unilever.ohub.spark.export.OutboundConfig
import com.unilever.ohub.spark.outbound.InMemStorage
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{BeforeAndAfter, Matchers}

class ContactPersonsAcmOldNewConcatIdJobSpec extends SparkJobSpec with TestContactPersons with TestOperators with BeforeAndAfter with Matchers {

  import spark.implicits._

  private val cp = defaultContactPerson.copy(isGoldenRecord = true, concatId = "NL~ARMSTRONG~12")
  private val contactPersons = Seq(cp, defaultContactPerson).toDataset
  private val hashes = {
    import spark.implicits._

    spark.createDataset[DomainEntityHash](Seq[DomainEntityHash]())
  }
  private val config = OutboundConfig(
    integratedInputFile = "integrated",
    hashesInputFile = Some("hash"),
    outboundLocation = UUID.randomUUID().toString
  )
  val storage = new InMemStorage(spark, contactPersons, hashes)

  after {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.delete(new Path(config.outboundLocation), true)
  }

  describe("Old new for CPs") {
    it("Should write correct csv for one golden record") {
      ContactPersonOldNewWriter.run(spark, config, storage)

      val result = storage.readFromCsv(config.outboundLocation, "\u00B6", true)

      result.collect().length shouldBe 1
      result.collect().head.toString() should include("NL~12~3~15,NL~ARMSTRONG~12")
    }
  }
}
