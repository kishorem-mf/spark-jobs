package com.unilever.ohub.spark.acm

import java.util.UUID

import com.unilever.ohub.spark.{ DefaultConfig, SparkJobWithDefaultConfig }
import com.unilever.ohub.spark.acm.model.UFSOrderLine
import com.unilever.ohub.spark.data.OrderRecord
import com.unilever.ohub.spark.generic.StringFunctions
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }

object OrderLineAcmConverter extends SparkJobWithDefaultConfig with AcmConverter {

  def transform(spark: SparkSession, orders: Dataset[OrderRecord]): Dataset[UFSOrderLine] = {
    import spark.implicits._

    orders.map(order ⇒ UFSOrderLine(
      ORDER_ID = order.orderConcatId,
      ORDERLINE_ID = UUID.randomUUID().toString,
      PRD_INTEGRATION_ID = StringFunctions.createConcatId(
        order.countryCode,
        order.source,
        order.refProductId
      ),
      QUANTITY = order.quantity,
      AMOUNT = order.orderValue.map(_.setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble),
      SAMPLE_ID = ""
    ))
  }

  override def run(spark: SparkSession, config: DefaultConfig, storage: Storage): Unit = {
    import spark.implicits._

    log.info(s"Generating order lines ACM csv file from [${config.inputFile}] to [${config.outputFile}]")

    val orderLines = storage.readFromParquet[OrderRecord](config.inputFile)
    val transformed = transform(spark, orderLines)

    // COUNTRY_CODE is not an existing column, therefore no country partitioning
    storage.writeToSingleCsv(transformed, config.outputFile, writeOptions)
  }
}
