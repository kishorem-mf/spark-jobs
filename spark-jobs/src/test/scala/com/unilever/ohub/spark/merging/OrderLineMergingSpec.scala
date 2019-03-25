package com.unilever.ohub.spark.merging

import java.sql.Timestamp

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.{ OrderLine, Product, TestOrderLines, TestProducts }
import org.apache.spark.sql.Dataset
import com.unilever.ohub.spark.SharedSparkSession.spark

class OrderLineMergingSpec extends SparkJobSpec with TestOrderLines with TestProducts {

  import spark.implicits._

  private val SUT = OrderLineMerging

  describe("orderLine merging") {

    it("a single orderline should get an ohubId and be marked as golden record") {
      val singleOrderLine = defaultOrderLine.copy(
        isGoldenRecord = true,
        orderConcatId = "order-1",
        ohubUpdated = Timestamp.valueOf("2018-10-05 10:30:45"),
        ohubId = None,
        concatId = "single-line",
        isActive = true)

      val input: Dataset[OrderLine] = Seq(
        singleOrderLine
      ).toDataset

      val previous: Dataset[OrderLine] = Seq[OrderLine]().toDataset

      val products: Dataset[Product] = Seq[Product]().toDataset

      val result = SUT.transform(spark, input, previous, products)
        .collect()
        .map(l ⇒ l.concatId -> (l.ohubId, l.isGoldenRecord)).toMap

      result("single-line")._1 shouldBe defined
      result("single-line")._2 shouldBe true
    }

    it("two orderlines of the same order should get the same ohubId") {
      val orderLine1 = defaultOrderLine.copy(
        isGoldenRecord = true,
        orderConcatId = "order-1",
        ohubUpdated = Timestamp.valueOf("2018-10-05 10:30:45"),
        ohubId = None,
        concatId = "line-1",
        isActive = true)

      val orderLine2 = defaultOrderLine.copy(
        isGoldenRecord = true,
        orderConcatId = "order-1",
        ohubUpdated = Timestamp.valueOf("2018-10-05 10:30:45"),
        ohubId = None,
        concatId = "line-2",
        isActive = true)

      val input: Dataset[OrderLine] = Seq(
        orderLine1,
        orderLine2
      ).toDataset

      val previous: Dataset[OrderLine] = Seq[OrderLine]().toDataset

      val products: Dataset[Product] = Seq[Product]().toDataset

      val result = SUT.transform(spark, input, previous, products)
        .collect()
        .map(l ⇒ l.concatId -> (l.ohubId, l.isGoldenRecord)).toMap

      result.size shouldBe 2
      result("line-1")._1 shouldBe defined
      result("line-1")._2 shouldBe true
      result("line-2")._2 shouldBe true

      result("line-1")._1 shouldBe result("line-2")._1
    }

    it("two orderlines of two distinct orders should get a different ohubId") {
      val orderLine1 = defaultOrderLine.copy(
        isGoldenRecord = true,
        orderConcatId = "order-1",
        ohubUpdated = Timestamp.valueOf("2018-10-05 10:30:45"),
        ohubId = None,
        concatId = "line-1",
        isActive = true)

      val orderLine2 = defaultOrderLine.copy(
        isGoldenRecord = true,
        orderConcatId = "order-2",
        ohubUpdated = Timestamp.valueOf("2017-09-01 10:30:45"),
        ohubId = None,
        concatId = "line-2",
        isActive = true)

      val input: Dataset[OrderLine] = Seq(
        orderLine1,
        orderLine2
      ).toDataset

      val previous: Dataset[OrderLine] = Seq[OrderLine]().toDataset

      val products: Dataset[Product] = Seq[Product]().toDataset

      val result = SUT.transform(spark, input, previous, products)
        .collect()
        .map(l ⇒ l.concatId -> (l.ohubId, l.isGoldenRecord)).toMap

      result.size shouldBe 2
      result("line-1")._1 shouldBe defined
      result("line-1")._2 shouldBe true

      result("line-1")._1 should not be result("line-2")._1
    }

    it("the most recent orderlines should be preserved and marked as golden record") {
      val newest1Record = defaultOrderLine.copy(
        isGoldenRecord = true,
        orderConcatId = "order-1",
        ohubUpdated = Timestamp.valueOf("2018-10-05 10:30:45"),
        concatId = "newest1",
        isActive = true)

      val newest2Record = defaultOrderLine.copy(
        isGoldenRecord = true,
        orderConcatId = "order-1",
        ohubUpdated = Timestamp.valueOf("2018-10-05 10:30:45"),
        concatId = "newest2",
        isActive = true)

      val oldestRecord = defaultOrderLine.copy(
        isGoldenRecord = true,
        orderConcatId = "order-1",
        ohubUpdated = Timestamp.valueOf("2018-09-05 10:30:45"),
        ohubId = Some("ohub-order-1"),
        concatId = "oldest",
        isActive = true)

      val goldenRecordWithoutDateUpdated = defaultOrderLine.copy(
        isGoldenRecord = true,
        orderConcatId = "order-2",
        ohubUpdated = Timestamp.valueOf("2018-08-08 09:09:30"),
        concatId = "other",
        isActive = true)

      val input: Dataset[OrderLine] = Seq(
        newest1Record,
        newest2Record
      ).toDataset

      val previous: Dataset[OrderLine] = Seq(
        oldestRecord,
        goldenRecordWithoutDateUpdated
      ).toDataset

      val products: Dataset[Product] = Seq[Product]().toDataset

      val result = SUT.transform(spark, input, previous, products)
        .collect()
        .map(l ⇒ l.concatId -> (l.ohubId, l.isGoldenRecord)).toMap

      result("other")._2 shouldBe true
      result("newest1") shouldBe (Some("ohub-order-1"), true)
      result("newest2") shouldBe (Some("ohub-order-1"), true)
    }

    it("Should drop old records when the same order is supplied in delta") {
      val unchangedRecord = defaultOrderLine.copy(
        orderConcatId = "order-2",
        comment = Some("1st"),
        concatId = "unchanged",
        isActive = false)

      val updatedRecord = defaultOrderLine.copy(
        orderConcatId = "order-1",
        comment = Some("2nd"),
        concatId = "oldie",
        isActive = true)

      val previous: Dataset[OrderLine] = spark.createDataset(Seq(
        updatedRecord,
        unchangedRecord
      ))
      val input: Dataset[OrderLine] = spark.createDataset(Seq(
        updatedRecord.copy(comment = Some("3rd"), ohubId = Some("newId"), isActive = false)
      ))

      val products: Dataset[Product] = Seq[Product]().toDataset

      val result = SUT.transform(spark, input, previous, products)
        .orderBy($"comment".asc)
        .collect();

      result.length shouldBe 2 // Previously ingested records for an orderId are removed, delta records to active(same as golden)
      result(0).isActive shouldBe false
      result(0).isGoldenRecord shouldBe true
      result(1).isActive shouldBe false
      result(1).isGoldenRecord shouldBe true
    }

    it("delta input orderline data is preserved in favor of integrated data while retaining ohubId") {
      val updatedRecord = defaultOrderLine.copy(
        orderConcatId = "order-1",
        ohubId = Some("oldId"),
        concatId = "updated",
        comment = Some("Calve"),
        isActive = true)

      val deletedRecord = defaultOrderLine.copy(
        orderConcatId = "order-1",
        concatId = "deleted",
        isActive = true)

      val newRecord = defaultOrderLine.copy(
        orderConcatId = "order-1",
        concatId = "new"
      )

      val unchangedRecord = defaultOrderLine.copy(
        orderConcatId = "order-1",
        concatId = "unchanged"
      )

      val notADeltaRecord = defaultOrderLine.copy(
        orderConcatId = "order-1",
        concatId = "notADelta"
      )

      val previous: Dataset[OrderLine] = spark.createDataset(Seq(
        updatedRecord,
        deletedRecord,
        unchangedRecord,
        notADeltaRecord
      ))
      val input: Dataset[OrderLine] = spark.createDataset(Seq(
        updatedRecord.copy(comment = Some("Unox"), ohubId = Some("newId"), isActive = false),
        unchangedRecord,
        newRecord
      ))

      val products: Dataset[Product] = Seq[Product]().toDataset

      val result = SUT.transform(spark, input, previous, products)
        .collect()
        .sortBy(_.concatId)

      result.length shouldBe 3 // Previously ingested records for an orderId are set to inactive, delta records to active(same as golden)
      result(0).concatId shouldBe "new"
      result(1).concatId shouldBe "unchanged"
      result(2).concatId shouldBe "updated"
      result(2).isActive shouldBe false
      result(2).isGoldenRecord shouldBe true
      result(2).comment shouldBe Some("Unox")
      result(2).ohubId shouldBe Some("oldId")
    }

    it("should set the reference to the right productOhubId") {
      val productConcatId = "NL~SNL~prod1"
      val productOhubId = "product-ohub-id"

      val recordWithValidProductConcatId = defaultOrderLine.copy(
        isGoldenRecord = false,
        ohubId = Some("oldId"),
        countryCode = "withProductId",
        concatId = s"withProductId~${defaultOrderLine.sourceName}~${defaultOrderLine.sourceEntityId}",
        productConcatId = productConcatId,
        productOhubId = None,
        comment = Some("Calve"))

      val recordWithUnknownProductConcatId = defaultOrderLine.copy(
        isGoldenRecord = true,
        ohubId = Some("oldId"),
        countryCode = "withoutProductId",
        concatId = s"withoutProductId~${defaultOrderLine.sourceName}~${defaultOrderLine.sourceEntityId}",
        productConcatId = "unknown-product-id",
        productOhubId = None,
        comment = Some("Calve"))

      val previous: Dataset[OrderLine] = spark.createDataset(Seq())
      val input: Dataset[OrderLine] = spark.createDataset(Seq(
        recordWithValidProductConcatId,
        recordWithUnknownProductConcatId
      ))

      val products: Dataset[Product] = Seq[Product](
        defaultProduct.copy(
          countryCode = "NL",
          sourceName = "SNL",
          sourceEntityId = "prod1",
          concatId = productConcatId,
          ohubId = Some(productOhubId)
        )
      ).toDataset

      SUT
        .transform(spark, input, previous, products)
        .collect()
        .map(r ⇒ r.countryCode -> r.productOhubId).toMap shouldBe Map(
          "withProductId" -> Some(productOhubId),
          "withoutProductId" -> None
        )
    }

    it("when no delta records have been provided, integrated should keep previous status") {
      val singleOrderLine = defaultOrderLine.copy(
        isGoldenRecord = false,
        orderConcatId = "order-1",
        ohubUpdated = Timestamp.valueOf("2018-10-05 10:30:45"),
        ohubId = None,
        concatId = "first-line",
        isActive = true)

      val input: Dataset[OrderLine] = Seq[OrderLine]().toDataset

      val previous: Dataset[OrderLine] = Seq(
        singleOrderLine,
        singleOrderLine.copy(orderConcatId = "order-1", concatId = "second-line", isGoldenRecord = true)
      ).toDataset

      val products: Dataset[Product] = Seq[Product]().toDataset

      val result = SUT.transform(spark, input, previous, products)
        .collect()
        .map(l ⇒ l.concatId -> (l.ohubId, l.isGoldenRecord)).toMap

      result("second-line")._2 shouldBe true
    }
  }
}
