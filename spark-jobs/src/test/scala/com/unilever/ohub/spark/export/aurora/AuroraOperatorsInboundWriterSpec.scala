package com.unilever.ohub.spark.export.aurora

import java.sql.Timestamp
import java.util.UUID

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.datalake.OperatorOutboundWriter
import com.unilever.ohub.spark.domain.entity.{Operator, TestOperators}
import com.unilever.ohub.spark.export.TargetType
import com.unilever.ohub.spark.export.domain.InMemStorage
import com.unilever.ohub.spark.{SparkJobSpec, export}
import org.apache.spark.sql.Dataset


class AuroraOperatorsInboundWriterSpec extends SparkJobSpec with TestOperators{

  import spark.implicits._

  private val SUT = OperatorOutboundWriter
  private val outboundLocation = UUID.randomUUID().toString+"/"
  private val config = export.OutboundConfig(
    integratedInputFile = "raw",
    outboundLocation = outboundLocation,
    targetType = TargetType.UDL,
    auroraCountryCodes = "NL;AT;TR",
    fromDate = "2020-12-28"
)


  describe("Aurora csv generation") {
    val cp1 = defaultOperator.copy(name = Some("a"), ohubId = Some("G"),
      dateUpdated = None,
      dateCreated = Some(Timestamp.valueOf("2015-09-30 14:23:05.0")),
      countryCode = "AT")

    val operators: Dataset[Operator] = Seq(cp1).toDataset
    // As InMemStorage has prevIntegrated as third parameter which is mandatory, we are passing the same integrated there
    val storage = new InMemStorage(spark, operators, operators)
    val inputFile = "src/test/resources/COMMON_OPERATORS.csv"
    val result = SUT.transformInboundFilesByDate(inputFile,config.fromDate,config,spark,storage) //.run(spark, config, storage)


    it("Should filter based on countryCodes") {
      val resultcsv = spark.read.option("sep",";").option("header","true").csv(config.outboundLocation + "KANGAROO/tr/operators/Processed/YYYY=2020/MM=12/DD=28/*.csv")
        resultcsv.count() shouldBe 6
    }
  }

}
