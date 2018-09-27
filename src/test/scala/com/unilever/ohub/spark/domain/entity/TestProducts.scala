package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

import com.unilever.ohub.spark.ingest.sifu.SifuProductResponse

object TestProducts extends TestProducts

trait TestProducts {

  // format: OFF
  lazy val defaultProduct: Product = Product(
    concatId = "country-code~source-name~source-entity-id",
    countryCode = "country-code",
    customerType = Product.customerType,
    dateCreated = None,
    dateUpdated = None,
    isActive = true,
    isGoldenRecord = true,
    ohubId = Some("ohub-id"),
    name = "product-name",
    sourceEntityId = "source-entity-id",
    sourceName = "source-name",
    ohubCreated = Timestamp.valueOf("2015-06-30 13:49:00.0"),
    ohubUpdated = Timestamp.valueOf("2015-06-30 13:49:00.0"),
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
    productId = None,
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
    additionalFields = Map(),
    ingestionErrors = Map()
  )

  lazy val sifuProductResponse: SifuProductResponse = SifuProductResponse(
    additives = None,
    allergens = Some(List("bevat vis", "bevat sulfiet")),
    availabilityHint = Some(1L),
    available = false,
    benefits = None,
    brandCode = Some("581"),
    brandName = Some("Knorr"),
    categoryCode = Some("1884"),
    categoryName = Some("Bouillons"),
    claim1 = None,
    claim2 = None,
    claim3 = None,
    claim4 = None,
    claimTitle = None,
    code = Some("25-NL-200194"),
    containerCode = Some("849"),
    containerName = Some("1 l"),
    country = Some("nl"),
    cuAvailable = Some(false),
    cuEanCode = Some("8712100662508"),
    cuLoyaltyPoints = Some(18),
    cuPriceInCents = Some(1781),
    dataSourceId = Some("25-NL-200194"),
    defaultPackagingType = None,
    description = None,
    dietetics = None,
    disclaimer1 = None,
    disclaimer2 = None,
    disclaimer3 = None,
    duAvailable = Some(true),
    duEanCode       = Some("8712100979392"),
    duLoyaltyPoints = Some(107),
    duOnlyProduct = false,
    duPriceInCents = Some(10686),
    howThisProductWillHelpYou = None,
    image1Id = Some("50209972"),
    image1Text = None,
    image2Id = Some("0"),
    image2Text = None,
    image3Id = Some("0"),
    image3Text = None,
    ingredients = Some("Geconcentreerde visbouillon (64%) (water, VISPOEDER), zout, gistextract, suiker, maltodextrine, uipoeder, citroensappoeder, gemodificeerd maïszetmeel, aroma's (bevat witte wijnaroma (bevat alcohol)), verdikkingsmiddel (xanthaangom), kruiden (laurierblad, tijm), specerijen (peper, venkelzaad). Kan weekdieren en schaaldieren bevatten."),
    isUnileverProduct = Some("1"),
    itemType = Some("product"),
    language = Some("NL"),
    lastModifiedDate = Some("20180216102700"),
    loyaltyReward = false,
    madeIn = None,
    mainAppVisualImageName = None,
    mainAppVisualImageUrl = None,
    name = Some("Knorr Professional Geconcentreerde Bouillon Vis"),
    nameSlug = Some("knorr-professional-geconcentreerde-bouillon-vis"),
    noteAddAllergen = None,
    noteNutrients = None,
    number = Some("29793901"),
    nutrientPortionValues = Some(List("0 kcal", "0.0 g", "0.0 g", "0.0 g", "0.0 g", "0.0 g", "0.0 g", "0 mg")),
    nutrientRules = Some(List("NEP pink main dish", "NEP purple main dish")),
    nutrientTypes = Some(List("energie kcal", "vetten", "vetten wv. verzadigde vetten", "koolhydraten", "koolhydraten wv. suikers", "voedingsvezels", "eiwitten", "natrium")),
    nutrientValues = Some(List("67 kcal", "0.5 g", "0.1 g", "8.1 g", "4.0 g", "1.5 g", "7.8 g", "8,200 mg")),
    orderScore = Some("1"),
    packagingCode = Some("3954"),
    packagingName = Some("6 x 1 L"),
    packshotResizedUrl = Some("https://sifu.unileversolutions.com/image/nl-NL/original/1/200/200/knorr-professional-geconcentreerde-bouillon-vis-50209972.jpg"),
    packshotUrl = Some("https://sifu.unileversolutions.com/image/nl-NL/original/1/knorr-professional-geconcentreerde-bouillon-vis-50209972.jpg"),
    portionSize = Some("0"),
    portionUnit = None,
    preparation = None,
    productCode = Some("25-NL-200194"),
    productCodes = Some(List("25-NL-200194")),
    productNumber = Some("29793901"),
    productType = Some("PRODUCT"),
    productiveness = Some("1L"),
    properties = Some("Knorr Professional Geconcentreerde Visbouillon is een vloeibare geconcentreerde bouillon waarmee gerechten op smaak kunnen worden gebracht. Het is een licht gekruide en gezouten geconcentreerde bouillon die makkelijk en snel de smaak van een gerecht kan versterken.   De voordelen: 1. De echte smaak van vis (geen smaakversterkers) 2. Makkelijk toe te voegen dankzij de vleibare textuur. 3. Te gebruiken in zowel warme als koude gerechten 4. Glutenvrij"),
    rtbClaim = None,
    rtbClaim1 = None,
    rtbClaim2 = None,
    rtbImageAfterUrl = None,
    rtbImageName = None,
    rtbImageNameAfter = None,
    rtbImageUrl = None,
    shelfLife = None,
    showCuInfoHint = Some(1),
    showDuInfoHint = Some(1),
    showGdaOnWebsite = Some("0"),
    slugifiedBrandName = Some("knorr"),
    slugifiedCategoryName = Some("bouillons"),
    slugifiedSubCategoryName = Some("vloeibaar"),
    solutionCopy = None,
    storage = None,
    subBrandCode = Some("1193"),
    subBrandName = Some("Professional"),
    subCategoryCode = Some("5361"),
    subCategoryName = Some("Vloeibaar"),
    tips = None,
    topProduct = false,
    truthCopy = None,
    truthVisualImageName = None,
    truthVisualImageUrl = None,
    url = Some("https://www.unileverfoodsolutions.nl/product/knorr-professional-geconcentreerde-bouillon-vis-25-NL-200194.html"),
    youtubeUrl = None
  )
  // format: ON
}

