package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.Product
import com.unilever.ohub.spark.ingest.CustomParsers._
import com.unilever.ohub.spark.ingest.{DomainTransformer, ProductEmptyParquetWriter}
import org.apache.spark.sql.Row

object ProductConverter extends CommonDomainGateKeeper[Product] with ProductEmptyParquetWriter {

  // scalastyle:off method.length
  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ Product = { transformer ⇒
    row ⇒
      import transformer._
      implicit val source: Row = row

      val ohubCreated =  new Timestamp(System.currentTimeMillis())

      Product(
        id = mandatory("id"),
        creationTimestamp = mandatory("creationTimestamp", toTimestamp),
        concatId = mandatory("concatId"),
        countryCode = mandatory("countryCode"),
        customerType = Product.customerType,
        dateCreated = optional("dateCreated", parseDateTimeUnsafe()),
        dateUpdated = optional("dateUpdated", parseDateTimeUnsafe()),
        isActive = mandatory("isActive", toBoolean),
        isGoldenRecord = false, // set in ProductMerging
        ohubId = None, // set in ProductMerging
        name = mandatory("name"),
        sourceEntityId = mandatory("sourceEntityId"),
        sourceName = mandatory("sourceName"),
        ohubCreated = ohubCreated,
        ohubUpdated = ohubCreated,
        // specific fields
        additives = List.empty,
        allergens = List.empty,
        availabilityHint = Option.empty,
        benefits = Option.empty,
        brandCode = optional("brandCode"),
        brandName = Option.empty,
        categoryByMarketeer = Option.empty,
        categoryCodeByMarketeer = Option.empty,
        categoryLevel1 = Option.empty,
        categoryLevel2 = Option.empty,
        categoryLevel3 = Option.empty,
        categoryLevel4 = Option.empty,
        categoryLevel5 = Option.empty,
        categoryLevel6 = Option.empty,
        categoryLevel7 = Option.empty,
        categoryLevel8 = Option.empty,
        categoryLevel9 = Option.empty,
        categoryLevel10 = Option.empty,
        categoryLevel11 = Option.empty,
        categoryLevel12 = Option.empty,
        categoryLevel13 = Option.empty,
        code = optional("code"),
        codeType = Some("MRDR"),
        consumerUnitLoyaltyPoints = Option.empty,
        consumerUnitPriceInCents = Option.empty,
        containerCode = Option.empty,
        containerName = Option.empty,
        currency = optional("currency"),
        defaultPackagingType = Option.empty,
        description = Option.empty,
        dietetics = List.empty,
        distributionUnitLoyaltyPoints = Option.empty,
        distributionUnitPriceInCents = Option.empty,
        eanConsumerUnit = optional("eanConsumerUnit"),
        eanDistributionUnit = optional("eanDistributionUnit"),
        hasConsumerUnit = Option.empty,
        hasDistributionUnit = Option.empty,
        imageId = Option.empty,
        ingredients = Option.empty,
        isAvailable = Option.empty,
        isDistributionUnitOnlyProduct = Option.empty,
        isLoyaltyReward = Option.empty,
        isTopProduct = Option.empty,
        isUnileverProduct = Option.empty,
        itemType = Option.empty,
        language = Option.empty,
        lastModifiedDate = Option.empty,
        nameSlug = Option.empty,
        number = Option.empty,
        nutrientTypes = List.empty,
        nutrientValues = List.empty,
        orderScore = Option.empty,
        packagingCode = Option.empty,
        packagingName = Option.empty,
        packshotUrl = Option.empty,
        portionSize = Option.empty,
        portionUnit = Option.empty,
        preparation = Option.empty,
        productCodes = List.empty,
        productType = Option.empty,
        solutionCopy = Option.empty,
        subBrandCode = optional("subBrandCode"),
        subBrandName = Option.empty,
        subCategoryByMarketeer = Option.empty,
        subCategoryCode = Option.empty,
        subCategoryName = Option.empty,
        `type` = optional("type"),
        unit = optional("unit"),
        unitPrice = optional("unitPrice", toBigDecimal),
        youtubeUrl = Option.empty,
        additionalFields = additionalFields,
        ingestionErrors = errors
      )
  }
}
