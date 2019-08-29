package com.unilever.ohub.spark.export.domain

import java.util.UUID

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.domain.DomainEntityHash
import com.unilever.ohub.spark.domain.entity.{Order, TestOrders}
import com.unilever.ohub.spark.export.TargetType.MEPS
import com.unilever.ohub.spark.outbound.InMemStorage
import com.unilever.ohub.spark.{SparkJobSpec, export}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.BeforeAndAfter

class OrderDomainExportWriterSpecs extends SparkJobSpec with BeforeAndAfter with TestOrders {

  import spark.implicits._

  private val pkOD1 = defaultOrder.copy(countryCode = "PK", sourceName = "NOT-ARMSTRONG", concatId = "onlyExportedOrder")
  private val pkOD2 = defaultOrder.copy(countryCode = "PK", sourceName = "ARMSTRONG")
  private val auOD1 = defaultOrder.copy(countryCode = "AU", sourceName = "NOT-ARMSTRONG")
  private val orders = Seq(pkOD1, pkOD2, auOD1).toDataset
  private val prevInteg = Seq[Order]().toDataset

  val SUT = Order.domainExportWriter.get

  val storage = new InMemStorage(spark, orders, prevInteg)

  private val config = export.OutboundConfig(
    integratedInputFile = "integratedFolder",
    outboundLocation = UUID.randomUUID().toString
  )

  after {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.delete(new Path(config.outboundLocation), true)
  }

  describe("OrderDomainExportWriter") {
    describe("MEPS export") {
      it("Should filter based on allowed countryCodes and sourcename") {
        val mepsConfig = config.copy(targetType = MEPS)

        SUT.export(orders, prevInteg, mepsConfig, spark)
        val result = storage.readFromCsv(mepsConfig.outboundLocation, new DomainExportOptions {}.delimiter, true).orderBy($"concatId".asc).collect()

        result.length shouldBe 1
        result(0).getString(result(0).fieldIndex("concatId")) shouldBe pkOD1.concatId
      }
    }
  }
}
