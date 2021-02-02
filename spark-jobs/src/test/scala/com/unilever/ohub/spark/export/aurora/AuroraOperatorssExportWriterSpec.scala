package com.unilever.ohub.spark.export.aurora

import java.sql.Timestamp
import java.util.UUID

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.domain.entity.{Operator, TestOperators}
import com.unilever.ohub.spark.export.TargetType
import com.unilever.ohub.spark.export.domain.InMemStorage
import com.unilever.ohub.spark.{SparkJobSpec, export}
import org.apache.spark.sql.{Dataset, Row}

class AuroraOperatorsExportWriterSpec extends SparkJobSpec with TestOperators {

  import spark.implicits._

  private val SUT = com.unilever.ohub.spark.export.aurora.OperatorOutboundWriter
  private val outboundLocation = UUID.randomUUID().toString+"/"
  private val config = export.OutboundConfig(
    integratedInputFile = "integrated",
    outboundLocation = outboundLocation,
    targetType = TargetType.AURORA,
    auroraCountryCodes = "AT;DE"
  )


  describe("Aurora parquet generation") {

    val cp1 = defaultOperator.copy(name = Some("a"), ohubId = Some("G"),
      dateUpdated = None,
      dateCreated = Some(Timestamp.valueOf("2015-09-30 14:23:05.0")),
      countryCode = "AT")

    val cp2 = defaultOperator.copy(name = Some("b"), ohubId = Some("S"),
      dateUpdated = None,
      dateCreated = Some(Timestamp.valueOf("2015-09-30 14:23:05.0")),
      countryCode = "DE"
    )
    val cp3 = defaultOperator.copy(name = Some("c"), ohubId = Some("V"),
      dateUpdated = None,
      dateCreated = Some(Timestamp.valueOf("2015-09-30 14:23:05.0")),
      countryCode = "NL"
    )
    val cp4 = defaultOperator.copy(name = Some("d"), ohubId = Some("W"),
      dateUpdated = None,
      dateCreated = Some(Timestamp.valueOf("2015-09-30 14:23:05.0")),
      countryCode = "NL"
    )

    val operators: Dataset[Operator] = Seq(cp1, cp2, cp3).toDataset
    // As InMemStorage has prevIntegrated as third parameter which is mandatory, we are passing the same integrated there
    val storage = new InMemStorage(spark, operators, operators)

    val result = SUT.run(spark, config, storage)

    it("Should filter based on countryCodes") {
      val resultparquet = spark.read.parquet(config.outboundLocation + "*/operators/Processed/operators.parquet")
      resultparquet.select("countryCode").distinct().count() shouldBe 2
    }

    it("Should not contain non aurora countries") {
      val resultparquet = spark.read.parquet(config.outboundLocation + "*/operators/Processed/operators.parquet")
      resultparquet.filter($"countryCode" === "NL").select($"countryCode").distinct().count() shouldBe 0
    }
  }

}
