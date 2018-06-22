package com.unilever.ohub.spark.dispatcher

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.dispatcher.model.DispatcherOrderLine
import com.unilever.ohub.spark.domain.entity.{ OrderLine, TestOrderLines }
import org.apache.spark.sql.Dataset

class OrderLineDispatcherConverterSpec extends SparkJobSpec with TestOrderLines {

  private val orderLineDispatcherConverter = OrderLineDispatcherConverter

  describe("order line dispatcher converter") {
    it("should convert a domain order line correctly into an dispatcher order line") {
      import spark.implicits._

      /**
       * Input file containing order line records
       */
      val input: Dataset[OrderLine] = {
        spark.createDataset(
          List(defaultOrderLine
            .copy(isGoldenRecord = false)
            .copy(isActive = false)
            .copy(ohubId = Some("randomId"))
            .copy(orderConcatId = "order-concat-id")
          )
        )
      }

      /**
       * There is no previous run containing order line records, so
       * we create an empty dataset
       */
      val emptyDataset: Dataset[OrderLine] = spark.emptyDataset[OrderLine]

      /**
       * Transformed Order Line records
       */
      val result: List[DispatcherOrderLine] = {
        orderLineDispatcherConverter.transform(spark, input, emptyDataset).collect().toList
      }

      result should contain(DispatcherOrderLine(
        AMOUNT = BigDecimal(0),
        CAMPAIGN_LABEL = None, // not available
        COMMENTS = None,
        ODL_INTEGRATION_ID = "country-code~source-name~source-entity-id",
        COUNTRY_CODE = "country-code",
        UNIT_PRICE_CURRENCY = None,
        DELETE_FLAG = true,
        LOYALTY_POINTS = None, // not available
        ODS_CREATED = "2015-06-30 13:49:00",
        ODS_UPDATED = "2015-06-30 13:49:00",
        ORD_INTEGRATION_ID = "order-concat-id",
        UNIT_PRICE = None,
        PRD_INTEGRATION_ID = "product-concat-id",
        QUANTITY = 0L,
        SOURCE = "source-name"
      ))
    }
  }
}
