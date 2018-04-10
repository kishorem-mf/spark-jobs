package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

import org.scalatest.{ Matchers, WordSpec }

class ProductSpec extends WordSpec with Matchers {

  "Product" should {
    "be created correctly (no exception is thrown)" when {
      "only valid data is provided" in {
        // format: OFF
        val product = Product(
          concatId = "concat-id",
          countryCode = "country-code",
          dateCreated = new Timestamp(System.currentTimeMillis()),
          dateUpdated = new Timestamp(System.currentTimeMillis()),
          isActive = true,
          isGoldenRecord = true,
          ohubId = None,
          name = "product-name",
          sourceEntityId = "source-entity-id",
          sourceName = "source-name",
          ohubCreated  = new Timestamp(System.currentTimeMillis()),
          ohubUpdated = new Timestamp(System.currentTimeMillis()),
          // specific fields
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
          code = None,
          codeType = None,
          consumerUnitLoyaltyPoints = None,
          consumerUnitPriceInCents = None,
          containerCode = None,
          containerName = None,
          currency = None,
          defaultPackagingType = None,
          description = None,
          dietetics = List.empty,
          distributionUnitLoyaltyPoints = None,
          distributionUnitPriceInCents = None,
          eanConsumerUnit = None,
          eanDistributionUnit = None,
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
          `type` = None,
          unit = None,
          unitPrice = None,
          youtubeUrl = None,
          // other fields
          ingestionErrors = Map()
        )
        // format: ON

        product.name shouldBe "product-name"
      }
    }
  }
}
