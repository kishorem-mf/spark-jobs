package com.unilever.ohub.spark.tsv2parquet.sifu

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.{ Product, TestProducts }
import com.unilever.ohub.spark.tsv2parquet.DomainGateKeeperDatasetSpec

class ProductConverterSpec extends DomainGateKeeperDatasetSpec[Product] with TestProducts {

  private[tsv2parquet] override val SUT = ProductConverter

  describe("sifu product converter") {
    it("should convert a product correctly from a valid api input") {

      runJobWith(Seq(TestProducts.sifuProductResponse), SUT.countryAndLanguages) { actualDataSet ⇒
        actualDataSet.count() shouldBe 229

        val actualProduct = actualDataSet.head()
        val expectedProduct = defaultProductRecord.copy(
          concatId = s"NL~SIFU~${actualProduct.sourceEntityId}",
          countryCode = "NL",
          customerType = Product.customerType,
          dateCreated = None,
          dateUpdated = None,
          isActive = true,
          isGoldenRecord = true,
          ohubId = actualProduct.ohubId,
          name = "Knorr Professional Geconcentreerde Bouillon Vis",
          sourceEntityId = actualProduct.sourceEntityId,
          sourceName = "SIFU",
          ohubCreated = actualProduct.ohubCreated,
          ohubUpdated = actualProduct.ohubUpdated,
          // specific fields
          additives = List.empty,
          allergens = List("bevat vis", "bevat sulfiet"),
          availabilityHint = Some(1L),
          benefits = None,
          brandCode = Some("581"),
          brandName = Some("Knorr"),
          categoryByMarketeer = Some("1884"),
          categoryCodeByMarketeer = Some("Bouillons"),
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
          code = Some("25-NL-200194"),
          codeType = Some("WEB"),
          consumerUnitLoyaltyPoints = Some(18),
          consumerUnitPriceInCents = Some(1781),
          containerCode = Some("849"),
          containerName = Some("1 l"),
          currency = None,
          defaultPackagingType = None,
          description = None,
          dietetics = List.empty,
          distributionUnitLoyaltyPoints = Some(107),
          distributionUnitPriceInCents = Some(10686),
          eanConsumerUnit = Some("8712100662508"),
          eanDistributionUnit = Some("8712100979392"),
          hasConsumerUnit = Some(false),
          hasDistributionUnit = Some(true),
          imageId = Some("50209972"),
          ingredients = Some("Geconcentreerde visbouillon (64%) (water, VISPOEDER), zout, gistextract, suiker, maltodextrine, uipoeder, citroensappoeder, gemodificeerd maïszetmeel, aroma's (bevat witte wijnaroma (bevat alcohol)), verdikkingsmiddel (xanthaangom), kruiden (laurierblad, tijm), specerijen (peper, venkelzaad). Kan weekdieren en schaaldieren bevatten."),
          isAvailable = Some(false),
          isDistributionUnitOnlyProduct = Some(false),
          isLoyaltyReward = Some(false),
          isTopProduct = Some(false),
          isUnileverProduct = Some("1"),
          itemType = Some("product"),
          language = Some("NL"),
          lastModifiedDate = Some(Timestamp.valueOf("2018-02-16 10:27:00")),
          nameSlug = Some("knorr-professional-geconcentreerde-bouillon-vis"),
          number = Some("29793901"),
          nutrientTypes = List("energie kcal", "vetten", "vetten wv. verzadigde vetten", "koolhydraten", "koolhydraten wv. suikers", "voedingsvezels", "eiwitten", "natrium"),
          nutrientValues = List("68 kcal", "0.5 g", "0.1 g", "8.1 g", "4.0 g", "1.5 g", "7.8 g", "8,200 mg"),
          orderScore = Some(1),
          packagingCode = Some("3954"),
          packagingName = Some("6 x 1 L"),
          packshotUrl = Some("https://sifu.unileversolutions.com/image/nl-NL/original/1/knorr-professional-geconcentreerde-bouillon-vis-50209972.jpg"),
          portionSize = Some(0),
          portionUnit = None,
          preparation = None,
          productCodes = List("25-NL-200194"),
          productId = actualProduct.productId,
          productType = Some("PRODUCT"),
          solutionCopy = None,
          subBrandCode = Some("1193"),
          subBrandName = Some("Professional"),
          subCategoryByMarketeer = None,
          subCategoryCode = Some("5361"),
          subCategoryName = Some("Vloeibaar"),
          `type` = None,
          unit = None,
          unitPrice = None,
          youtubeUrl = None,
          // other fields
          additionalFields = Map(),
          ingestionErrors = Map()
        )

        actualProduct shouldBe expectedProduct
      }
    }
  }
}
