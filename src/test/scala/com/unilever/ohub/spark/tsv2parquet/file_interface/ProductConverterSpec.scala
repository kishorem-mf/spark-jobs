package com.unilever.ohub.spark.tsv2parquet.file_interface

import java.sql.Timestamp
import java.util.UUID

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.domain.entity.{ Product, TestProducts }
import com.unilever.ohub.spark.storage.Storage

class ProductConverterSpec extends SparkJobSpec with TestProducts {
  describe("running") {
    it("should not throw any expection on a valid file") {
      val mockStorage = mock[Storage]

      val inputFile = "src/test/resources/FILE_PRODUCTS.csv"
      val outputFile = ""
      (mockStorage.readFromCsv _).expects(inputFile, "‰", true).returns(
        spark
          .read
          .option("header", value = true)
          .option("sep", "‰")
          .option("inferSchema", value = false)
          .csv(inputFile)
      )

      val expectedProduct = defaultProductRecord.copy(
        concatId = "AU~WUFOO~P1234",
        countryCode = "AU",
        dateCreated = Timestamp.valueOf("2015-06-30 13:47:00"),
        dateUpdated = Timestamp.valueOf("2015-06-30 13:48:00"),
        isActive = true,
        isGoldenRecord = true,
        ohubId = Some(UUID.randomUUID().toString), // can't access real value
        name = "KNORR CHICKEN POWDER(D) 12X1kg",
        sourceEntityId = "P1234",
        sourceName = "WUFOO",
        ohubCreated = new Timestamp(System.currentTimeMillis()), // can't access real value
        ohubUpdated = new Timestamp(System.currentTimeMillis()), // can't access real value
        code = Some("201119"),
        codeType = Some("MRDR"),
        currency = Some("GBP"),
        eanConsumerUnit = Some("812234000000"),
        eanDistributionUnit = Some("112234000000"),
        `type` = Some("Product"),
        unit = Some("Cases"),
        unitPrice = Some(BigDecimal(4))
      )

      (mockStorage.writeToParquet _).expects(*, outputFile, Seq("countryCode")).onCall { (resultDataSet, _, _) ⇒
        val actualProduct: Product = resultDataSet.head.asInstanceOf[Product]

        actualProduct shouldBe expectedProduct.copy(ohubId = actualProduct.ohubId, ohubCreated = actualProduct.ohubCreated, ohubUpdated = actualProduct.ohubUpdated)
      }

      ProductConverter.run(spark, (inputFile, outputFile), mockStorage)
    }
  }
}
