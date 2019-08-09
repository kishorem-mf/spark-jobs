package com.unilever.ohub.spark.export.domain

import java.util.UUID

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.domain.DomainEntityHash
import com.unilever.ohub.spark.domain.entity.{Order, OrderLine, TestOrderLines, TestOrders}
import com.unilever.ohub.spark.export.TargetType.MEPS
import com.unilever.ohub.spark.outbound.InMemStorage
import com.unilever.ohub.spark.{SparkJobSpec, export}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.BeforeAndAfter

class OrderLineDomainExportWriterSpecs extends SparkJobSpec with BeforeAndAfter with TestOrderLines {

  import spark.implicits._

  private val pkOL1 = defaultOrderLine.copy(countryCode = "PK", sourceName = "NOT-ARMSTRONG", concatId = "onlyExportedOrderLine")
  private val pkOL2 = defaultOrderLine.copy(countryCode = "PK", sourceName = "ARMSTRONG")
  private val auOL1 = defaultOrderLine.copy(countryCode = "AU", sourceName = "NOT-ARMSTRONG")
  private val orderlines = Seq(pkOL1, pkOL2, auOL1).toDataset
  private val hashes = Seq[DomainEntityHash]().toDataset

  val SUT = OrderLine.domainExportWriter.get

  val storage = new InMemStorage(spark, orderlines, hashes)

  private val config = export.OutboundConfig(
    integratedInputFile = "integratedFolder",
    outboundLocation = UUID.randomUUID().toString
  )

  after {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.delete(new Path(config.outboundLocation), true)
  }

  describe("OrderlinePersonDomainExportWriter") {
    describe("MEPS export") {
      it("Should filter based on allowed countryCodes and sourcename") {
        val mepsConfig = config.copy(targetType = MEPS)

        SUT.export(orderlines, hashes, mepsConfig, spark)
        val result = storage.readFromCsv(mepsConfig.outboundLocation, new DomainExportOptions {}.delimiter, true).orderBy($"concatId".asc).collect()

        result.length shouldBe 1
        result(0).getString(result(0).fieldIndex("concatId")) shouldBe pkOL1.concatId
      }
    }
  }
}
