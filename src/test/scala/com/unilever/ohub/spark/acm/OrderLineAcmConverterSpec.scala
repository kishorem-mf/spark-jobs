package com.unilever.ohub.spark.acm

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.acm.model.UFSOrderLine
import org.apache.spark.sql.Dataset
import com.unilever.ohub.spark.domain.entity.{ OrderLine, TestOrderLines }

class OrderLineAcmConverterSpec extends SparkJobSpec with TestOrderLines {

  private[acm] val SUT = OrderLineAcmConverter

  describe("OrderLine acm delta converter") {
    it("should convert a domain OrderLine correctly into an acm converter containing only delta records") {
      import spark.implicits._

      val updatedRecord = defaultOrderLine.copy(
        isGoldenRecord = true,
        countryCode = "updated",
        concatId = s"updated~${defaultOrderLine.sourceName}~${defaultOrderLine.sourceEntityId}")

      val newRecord = defaultOrderLine.copy(
        isGoldenRecord = true,
        countryCode = "new",
        concatId = s"new~${defaultOrderLine.sourceName}~${defaultOrderLine.sourceEntityId}"
      )

      val unchangedRecord = defaultOrderLine.copy(
        isGoldenRecord = true,
        countryCode = "unchanged",
        concatId = s"unchanged~${defaultOrderLine.sourceName}~${defaultOrderLine.sourceEntityId}"
      )

      val notADeltaRecord = defaultOrderLine.copy(
        isGoldenRecord = true,
        countryCode = "notADelta",
        concatId = s"notADelta~${defaultOrderLine.sourceName}~${defaultOrderLine.sourceEntityId}"
      )

      val previous: Dataset[OrderLine] = spark.createDataset(Seq(
        updatedRecord,
        unchangedRecord,
        notADeltaRecord
      ))
      val input: Dataset[OrderLine] = spark.createDataset(Seq(
        updatedRecord.copy(countryCode = "updaaated"),
        unchangedRecord,
        newRecord
      ))

      val result = SUT.transform(spark, input, previous)
        .collect()
        .sortBy(_.ORDERLINE_ID)

      result.length shouldBe 3
      //   assert(result.head.COUNTRY_CODE == Some("deleted"))
      //   assert(result.head.DELETE_FLAG == Some("Y"))
      //   assert(result(1).COUNTRY_CODE == Some("new"))
      //   assert(result(2).COUNTRY_CODE == Some("updated"))
      //   assert(result(2).OrderLine_NAME.contains("Unox"))
    }
  }

  describe("OrderLine acm converter") {
    it("should convert a domain OrderLine correctly into an acm UFSOrderLine") {
      import spark.implicits._

      val input: Dataset[OrderLine] = spark.createDataset(Seq(defaultOrderLine.copy(isGoldenRecord = true)))
      val result = SUT.transform(spark, input, spark.emptyDataset[OrderLine])

      result.count() shouldBe 1

      val actualUFSOrderLine = result.head()
      val expectedUFSOrderLine = UFSOrderLine(
        ORDERLINE_ID = "country-code~source-name~source-entity-id",
        ORD_INTEGRATION_ID = "country-code~source-name~source-entity-id",
        QUANTITY = 0L,
        AMOUNT = BigDecimal(0.0),
        LOYALTY_POINTS = None, // Some(BigDecimal(0.0))
        PRD_INTEGRATION_ID = "product-concat-id",
        SAMPLE_ID = "",
        CAMPAIGN_LABEL = None,
        COMMENTS = Some(""),
        DELETED_FLAG = "N"
      )

      actualUFSOrderLine shouldBe expectedUFSOrderLine
    }
  }
}
