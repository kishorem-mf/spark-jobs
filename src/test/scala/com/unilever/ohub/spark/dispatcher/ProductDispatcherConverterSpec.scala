package com.unilever.ohub.spark.dispatcher

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.dispatcher.model.DispatcherProduct
import com.unilever.ohub.spark.domain.entity.Product
import com.unilever.ohub.spark.domain.entity.TestProducts
import org.apache.spark.sql.Dataset

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
        BRAND = None,
        PRD_INTEGRATION_ID = "country-code~source-name~source-entity-id",
        COUNTRY_CODE = "country-code",
        UNIT_PRICE_CURRENCY = None,
        EAN_CODE = None,
        EAN_CODE_DISPATCH_UNIT = None,
        DELETE_FLAG = false,
        PRODUCT_NAME = "product-name",
        CREATED_AT = "2015-06-30 13:49:00",
        UPDATED_AT = "2015-06-30 13:49:00",
        MRDR_CODE = None,
        SOURCE = "source-name",
        SUB_BRAND = None,
        SUB_CATEGORY = None,
        ITEM_TYPE = None,
        UNIT = None,
        UNIT_PRICE = None,
        CATEGORY = None
      ))
    }
  }
}
