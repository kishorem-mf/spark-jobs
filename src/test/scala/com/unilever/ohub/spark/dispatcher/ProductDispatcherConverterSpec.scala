package com.unilever.ohub.spark.dispatcher

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.dispatcher.model.DispatcherProduct
import com.unilever.ohub.spark.domain.entity.Product
import com.unilever.ohub.spark.domain.entity.TestProducts
import org.apache.spark.sql.Dataset
import cats.syntax.option._

class ProductDispatcherConverterSpec extends SparkJobSpec with TestProducts {

  private val productDispatcherConverter = ProductDispatcherConverter

  describe("product dispatcher converter") {
    it("should convert a domain product correctly into an dispatcher product") {
      import spark.implicits._

      /**
       * Input file containing product records
       */
      val input: Dataset[Product] = {
        spark.createDataset(
          List(defaultProduct)
        )
      }

      /**
       * There is no previous run containing product records, so
       * we create an empty dataset
       */
      val emptyDataset: Dataset[Product] = spark.emptyDataset[Product]

      /**
       * Transformed DispatcherProduct
       */
      val result: List[DispatcherProduct] = {
        productDispatcherConverter.transform(spark, input, emptyDataset).collect().toList
      }

      result should contain(DispatcherProduct(
        BRAND = none,
        PRD_INTEGRATION_ID = "country-code~source-name~source-entity-id",
        COUNTRY_CODE = "country-code",
        UNIT_PRICE_CURRENCY = none,
        EAN_CODE = none,
        EAN_CODE_DISPATCH_UNIT = none,
        DELETE_FLAG = false,
        PRODUCT_NAME = "product-name",
        CREATED_AT = "2015-06-30 13:49:00",
        UPDATED_AT = "2015-06-30 13:49:00",
        MRDR_CODE = none,
        SOURCE = "source-name",
        SUB_BRAND = none,
        SUB_CATEGORY = none,
        ITEM_TYPE = none,
        UNIT = none,
        UNIT_PRICE = none,
        CATEGORY = none
      ))
    }
  }
}
