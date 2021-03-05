package com.unilever.ohub.spark.export.bdl

import java.sql.Timestamp
import java.util.UUID

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.domain.entity.{Operator, TestOperators}
import com.unilever.ohub.spark.export.businessdatalake.OperatorDLWriter
import com.unilever.ohub.spark.export.domain.InMemStorage
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{SparkJobSpec, export}
import org.apache.spark.sql.{Dataset, Row, SaveMode}

class BdlOperatorExportWriterSpec extends SparkJobSpec with TestOperators {
  import spark.implicits._
  private val SUT = OperatorDLWriter
  private val outboundLocation = UUID.randomUUID().toString+"/"
  private val config = export.businessdatalake.DataLakeConfig(
    integratedInputFile = "raw",
    entityName = "Operator",
    outboundLocation = outboundLocation,
    countryCodes = "DE;GB"
  )
  describe("BDL Parquet generation") {
    val op = defaultOperator.copy(name = Some("a"), ohubId = Some("G"),
      dateUpdated = None,
      dateCreated = Some(Timestamp.valueOf("2015-09-30 14:23:05.0")),
      countryCode = "DE")

    val operators: Dataset[Operator] = Seq(op).toDataset
    val prevIntegrated = Seq(defaultOperator).toDataset
    val mockStorage = mock[Storage]
    // let's not fail here, but do proper assertions in the assertion function
    val result = SUT.splitAndWriteParquetFiles(config.entityName, operators, getClass.getResource("/rexlite/")
      .getPath + "2020-09-16/operators.parquet", config, spark, mockStorage)

    }
}
