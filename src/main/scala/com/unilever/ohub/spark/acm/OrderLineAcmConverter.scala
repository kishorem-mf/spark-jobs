package com.unilever.ohub.spark.acm

import java.util.UUID

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }

case class OrderLine(
  ORDER_CONCAT_ID: String,
  QUANTITY: Int,
  ORDER_LINE_VALUE: Double,
  COUNTRY_CODE: String,
  SOURCE: String,
  REF_PRODUCT_ID: String
)

case class UfsOrderLine(
  ORDERLINE_ID: String,
  ORDER_ID: String,
  QUANTITY: Int,
  AMOUNT: Double,
  PRD_INTEGRATION_ID: String,
  SAMPLE_ID: String
)

object OrderLineAcmConverter extends SparkJob {
  def transform(spark: SparkSession, orderLines: Dataset[OrderLine]): Dataset[UfsOrderLine] = {
    import spark.implicits._

    orderLines.map(orderLine => UfsOrderLine(
      ORDERLINE_ID = UUID.randomUUID().toString,
      ORDER_ID = orderLine.ORDER_CONCAT_ID,
      PRD_INTEGRATION_ID = orderLine.COUNTRY_CODE + '~' + orderLine.SOURCE + '~' + orderLine.REF_PRODUCT_ID,
      QUANTITY = orderLine.QUANTITY,
      AMOUNT = BigDecimal(orderLine.ORDER_LINE_VALUE).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble,
      SAMPLE_ID = ""
    ))
  }

  override val neededFilePaths = Array("INPUT_FILE", "OUTPUT_FILE")

  override def run(spark: SparkSession, filePaths: scala.Product, storage: Storage): Unit = {
    import spark.implicits._

    val (inputFile: String, outputFile: String) = filePaths

    log.info(s"Generating order lines ACM csv file from [$inputFile] to [$outputFile]")

    val orderLines = storage
      .readFromParquet[OrderLine](
      inputFile,
      selectColumns =
        $"ORDER_CONCAT_ID",
        $"QUANTITY",
        $"ORDER_LINE_VALUE",
        $"COUNTRY_CODE",
        $"SOURCE",
        $"REF_PRODUCT_ID"
    )

    val transformed = transform(spark, orderLines)

    storage
      .writeToCSV(transformed, outputFile)
  }
}
