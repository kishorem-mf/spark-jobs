package com.unilever.ohub.spark.tsv2parquet.file_interface

import java.sql.Timestamp

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.domain.entity.Product
import com.unilever.ohub.spark.storage.Storage

class ProductConverterSpec extends SparkJobSpec {

  import spark.implicits._

  describe("running") {
    ignore("should not throw any expection on a valid file") {
      val mockStorage = mock[Storage]

      val inputFile = "src/test/resources/FILE_PRODUCTS.csv"
      val outputFile = ""
      (mockStorage.readFromCsv _).expects(inputFile, "‰", true).returns(
        spark
          .read
          .option("header", true)
          .option("sep", "‰")
          .option("inferSchema", value = false)
          .csv(inputFile)
      )

      val ds = Product(
        concatId = s"AU~WUFOO~P1234",
        countryCode = "AU",
        dateCreated = Timestamp.valueOf("2015-06-30 13:47:00"),
        dateUpdated = Timestamp.valueOf("2015-06-30 13:48:00"),
        isActive = true,
        isGoldenRecord = false,
        ohubId = None,
        name = "KNORR CHICKEN POWDER(D) 12X1kg",
        sourceEntityId = "P1234",
        sourceName = "WUFOO",
        ohubCreated = new Timestamp(System.currentTimeMillis()),
        ohubUpdated = new Timestamp(System.currentTimeMillis()),
        additives = List.empty,
        allergens = List.empty,
        availabilityHint = None,
        benefits = None,
        brandCode = None,
        brandName = None,
        categoryByMarketeer = None,
        categoryCodeByMarketeer = None,
        categoryLevel1 = None,
        categoryLevel2 = None,
        categoryLevel3 = None,
        categoryLevel4 = None,
        categoryLevel5 = None,
        categoryLevel6 = None,
        categoryLevel7 = None,
        categoryLevel8 = None,
        categoryLevel9 = None,
        categoryLevel10 = None,
        categoryLevel11 = None,
        categoryLevel12 = None,
        categoryLevel13 = None,
        code = Some("201119"),
        codeType = Some("MRDR"),
        consumerUnitLoyaltyPoints = None,
        consumerUnitPriceInCents = None,
        containerCode = None,
        containerName = None,
        currency = Some("GBP"),
        defaultPackagingType = None,
        description = None,
        dietetics = List.empty,
        distributionUnitLoyaltyPoints = None,
        distributionUnitPriceInCents = None,
        eanConsumerUnit = Some("812234000000"),
        eanDistributionUnit = Some("112234000000"),
        hasConsumerUnit = None,
        hasDistributionUnit = None,
        imageId = None,
        ingredients = None,
        isAvailable = None,
        isDistributionUnitOnlyProduct = None,
        isLoyaltyReward = None,
        isTopProduct = None,
        isUnileverProduct = None,
        itemType = None,
        language = None,
        lastModifiedDate = None,
        nameSlug = None,
        number = None,
        nutrientTypes = List.empty,
        nutrientValues = List.empty,
        orderScore = None,
        packagingCode = None,
        packagingName = None,
        packshotUrl = None,
        portionSize = None,
        portionUnit = None,
        preparation = None,
        productCodes = List.empty,
        productType = None,
        solutionCopy = None,
        subBrandCode = None,
        subBrandName = None,
        subCategoryByMarketeer = None,
        subCategoryCode = None,
        subCategoryName = None,
        `type` = Some("Product"),
        unit = Some("Cases"),
        unitPrice = Some(4),
        youtubeUrl = None,
        ingestionErrors = Map()
      ).toDataset

      (mockStorage.writeToParquet _).expects(ds, outputFile, Seq("countryCode"))

      ProductConverter.run(spark, (inputFile, outputFile), mockStorage)
    }
  }
}
