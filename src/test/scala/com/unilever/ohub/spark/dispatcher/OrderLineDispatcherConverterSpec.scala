package com.unilever.ohub.spark.dispatcher

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.dispatcher.model.DispatcherOrderLine
import com.unilever.ohub.spark.domain.entity.{ OrderLine, TestOrderLines }
import org.apache.spark.sql.Dataset
import cats.syntax.option._

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
        AMOUNT = 0,
        CAMPAIGN_LABEL = "campaign-label",
        COMMENTS = none,
        ODL_INTEGRATION_ID = "country-code~source-name~source-entity-id",
        COUNTRY_CODE = "country-code",
        UNIT_PRICE_CURRENCY = none,
        DELETE_FLAG = true,
        LOYALTY_POINTS = 123L,
        ODS_CREATED = "2015-06-30 13:49:00",
        ODS_UPDATED = "2015-06-30 13:49:00",
        ORD_INTEGRATION_ID = "order-concat-id",
        UNIT_PRICE = none,
        PRD_INTEGRATION_ID = "product-concat-id",
        QUANTITY = 0L,
        SOURCE = "source-name"
      ))
    }
  }
}
