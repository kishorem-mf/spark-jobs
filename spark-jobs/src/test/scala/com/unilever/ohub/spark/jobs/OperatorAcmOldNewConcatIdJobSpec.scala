package com.unilever.ohub.spark.jobs

import java.util.UUID

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.{Operator, TestContactPersons, TestOperators}
import com.unilever.ohub.spark.export.OutboundConfig
import com.unilever.ohub.spark.outbound.InMemStorage
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{BeforeAndAfter, Matchers}

class OperatorAcmOldNewConcatIdJobSpec extends SparkJobSpec with TestContactPersons with TestOperators with BeforeAndAfter with Matchers {

  import spark.implicits._

  private val op = defaultOperator.copy(isGoldenRecord = true, concatId = "BE~KANGAROO~14")
  private val operators = Seq(op, defaultOperator).toDataset
  private val prevInteg = {
    import spark.implicits._

    spark.createDataset[Operator](Seq[Operator]())
  }

  private val config = OutboundConfig(
    integratedInputFile = "integrated",
    outboundLocation = UUID.randomUUID().toString
  )
  val storage = new InMemStorage(spark, operators, prevInteg)

  after {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.delete(new Path(config.outboundLocation), true)
  }

  describe("Old new for operators") {
    it("Should write correct csv for one golden record") {
      OperatorOldNewWriter.run(spark, config, storage)

      val result = storage.readFromCsv(config.outboundLocation, "\u00B6", true)

      result.collect().length shouldBe 1
      result.collect().head.toString() should include("BE~14~1~9,BE~KANGAROO~14")
    }
  }
}
