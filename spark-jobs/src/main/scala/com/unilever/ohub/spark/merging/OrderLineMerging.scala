package com.unilever.ohub.spark.merging

import java.util.UUID

import com.unilever.ohub.spark.domain.entity.{ OrderLine, Product }
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{ collect_list, first, lit }
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

case class OrderLineMergingConfig(
    orderLineInputFile: String = "order-input-file",
    previousIntegrated: String = "previous-integrated-orderlines",
    productsIntegrated: String = "products-input-file",
    outputFile: String = "path-to-output-file"
) extends SparkJobConfig

// Technically not really order MERGING, but we need to update foreign key IDs in the other records
object OrderLineMerging extends SparkJob[OrderLineMergingConfig] {

  def setOhubId(orderLines: Seq[OrderLine]): Seq[OrderLine] = {
    val ohubId: String = orderLines
      .find(_.ohubId.isDefined)
      .flatMap(_.ohubId)
      .getOrElse(UUID.randomUUID().toString)

    orderLines.map(l ⇒ l.copy(ohubId = Some(ohubId)))
  }

  def transform(
    spark: SparkSession,
    orderLines: Dataset[OrderLine],
    previousIntegrated: Dataset[OrderLine],
    products: Dataset[Product]
  ): Dataset[OrderLine] = {
    import spark.implicits._

    val allOrderLines = previousIntegrated
      .withColumn("inDelta", lit(0)) // false --> Just to be sure, use a number for internal column
      .union(orderLines
        .withColumn("inDelta", lit(1)) // true
      )

    val w = Window.partitionBy($"orderConcatId").orderBy($"inDelta".desc) // true(=1) first is existent
    val w2 = Window.partitionBy($"orderConcatId").orderBy($"ohubId".desc_nulls_last)

    val allOrderLinesGrouped =
      allOrderLines
        .withColumn("firstInDelta", first('inDelta).over(w))
        .withColumn("ohubId", first('ohubId).over(w2)) // Copy ohubId from older version
        .withColumn("isActive", $"inDelta" === $"firstInDelta") // Only record provided in the delta are set on active, other inactive
        .withColumn("isGoldenRecord", $"inDelta" === $"firstInDelta") // Same as isActive column
        .drop($"inDelta")
        .as[OrderLine]
        .map(l ⇒ l.orderConcatId -> l)
        .toDF("orderConcatId", "orderLine")
        .groupBy($"orderConcatId")
        .agg(collect_list("orderLine").as("orderlines"))
        .as[(String, Seq[OrderLine])]
        .map(_._2)
        .flatMap(setOhubId)

    allOrderLinesGrouped
      .joinWith(products, $"productConcatId" === products("concatId"), "left")
      .map {
        case (order, product) ⇒
          if (product == null) order
          else order.copy(productOhubId = product.ohubId)
      }
  }

  override private[spark] def defaultConfig = OrderLineMergingConfig()

  override private[spark] def configParser(): OptionParser[OrderLineMergingConfig] =
    new scopt.OptionParser[OrderLineMergingConfig]("Order merging") {
      head("merges orders into an integrated order output file.", "1.0")
      opt[String]("orderLineInputFile") required () action { (x, c) ⇒
        c.copy(orderLineInputFile = x)
      } text "orderLineInputFile is a string property"
      opt[String]("previousIntegrated") optional () action { (x, c) ⇒
        c.copy(previousIntegrated = x)
      } text "previousIntegrated is a string property"
      opt[String]("productsIntegrated") optional () action { (x, c) ⇒
        c.copy(productsIntegrated = x)
      } text "productsIntegrated is a string property"
      opt[String]("outputFile") required () action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"

      version("1.0")
      help("help") text "help text"
    }

  override def run(spark: SparkSession, config: OrderLineMergingConfig, storage: Storage): Unit = {
    val orderRecords = storage.readFromParquet[OrderLine](config.orderLineInputFile)
    val previousIntegrated = storage.readFromParquet[OrderLine](config.previousIntegrated)
    val products = storage.readFromParquet[Product](config.productsIntegrated)

    val transformed = transform(spark, orderRecords, previousIntegrated, products)

    storage.writeToParquet(transformed, config.outputFile)
  }
}
