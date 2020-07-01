package com.unilever.ohub.spark.merging
import com.unilever.ohub.spark.{SparkJob, SparkJobConfig}
import com.unilever.ohub.spark.domain.entity.{Product, ProductSifu}
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.ingest.CustomParsers._
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.apache.spark.sql.functions.{col, upper,array_union}
import scopt.OptionParser

case class UpdateProductsConfig(
                                 currentIntegratedProducts: String = "product-input-file",
                                 productsSifuInputFile: String = "product-sifu-input-file",
                                 outputFile: String = "path-to-output-file"
                               ) extends SparkJobConfig

object UpdateProducts extends SparkJob[UpdateProductsConfig] {

  def transform(
                 spark: SparkSession,
                 products_sifu: Dataset[ProductSifu],
                 integratedProducts: Dataset[Product]
               ): Dataset[Product] = {

    implicit val sparkSession:SparkSession = spark
    import spark.implicits._
    implicit val sifuProductEncoder: Encoder[ProductSifu] = Encoders.product[ProductSifu]
    val mapDataRcn = Map("brandName" -> "brandName","youtubeUrl" -> "youtubeUrl","language" -> "language"
      ,"lastModifiedDate" -> "lastModifiedDate","brandCode" -> "brandCode","subBrandName" -> "subBrandName"
      ,"ingredients" -> "ingredients","packagingName" -> "packagingName","productType" -> "productType"
      ,"url" -> "url","loyaltyReward" -> "isLoyaltyReward","categoryCode"->"categoryCodeByMarketeer"
      ,"categoryName"->"categoryByMarketeer","subCategoryCode"->"subCategoryCode","subCategoryName"->"subCategoryName"
      ,"packagingCode"->"packagingCode","image1Id"->"imageId","packshotUrl"->"imageUrl","allergens"->"allergens"
      ,"cuPriceInCents"->"consumerUnitPriceInCents","duPriceInCents"->"distributionUnitPriceInCents"
      ,"nutrientTypes" ->"nutrientTypes","nutrientValues" ->"nutrientValues","packshotResizedUrl"->"previewImageUrl"
      ,"name"->"name","itemType"->"itemType","isUnileverProduct"->"isUnileverProduct","convenienceLevel"->"convenienceLevel"
      ,"description"->"description"
    )
    val mapDataPnir = Map("brandName" -> "brandName","youtubeUrl" -> "youtubeUrl","language" -> "language"
      ,"lastModifiedDate" -> "lastModifiedDate","brandCode" -> "brandCode","subBrandName" -> "subBrandName"
      ,"ingredients" -> "ingredients","packagingName" -> "packagingName","productType" -> "productType"
      ,"url" -> "url","loyaltyReward" -> "isLoyaltyReward","categoryCodes_1"->"categoryCodeByMarketeer"
      ,"categoryName"->"categoryByMarketeer","packagingCode"->"packagingCode","subCategoryCodes_1"->"subCategoryCode"
      ,"cuPriceInCents"->"consumerUnitPriceInCents","dachClaimFooterTexts"->"allergens","productName"->"name"
      ,"duPriceInCents"->"distributionUnitPriceInCents","subCategoryNames_1"->"subCategoryName"
      ,"extractedPreviewImageUrl"->"previewImageUrl","extractedImageId"->"imageId","extractedImageUrl"->"imageUrl"
      ,"extractedNutritionalType" ->"nutrientTypes","extractedNutritionalValue" ->"nutrientValues","description" -> "description"
    )
    val integratedPnir=integratedProducts.filter($"countryCode".isin("AT","CH","DE"))
    val integratedRcn=integratedProducts.filter(!$"countryCode".isin("AT","CH","DE"))
    val joinSifuProductsPnir = joinWith(integratedPnir, products_sifu,mapDataPnir)
    val joinSifuProductsRcn= joinWith(integratedRcn, products_sifu,mapDataRcn)
    val updatedSifu=joinSifuProductsPnir.unionByName(joinSifuProductsRcn).cache.as[Product]
    val deltaWithoutSifu = integratedProducts
      .join(updatedSifu, Seq("concatId"), JoinType.LeftAnti)
      .as[Product]
    deltaWithoutSifu
      .unionByName(updatedSifu)
      .as[Product]
  }
  private def joinWith(
                                     integratedProducts: Dataset[Product],
                                     products_sifu_limited: Dataset[ProductSifu],
                                     mapData: Map[String,String]
                                   ):Dataset[Product] = {
   implicit val sifuProductEncoder: Encoder[ProductSifu] = Encoders.product[ProductSifu]
   implicit val productEncoder: Encoder[Product] = Encoders.product[Product]
   val updatedWithSifu = mapData
     .foldLeft(integratedProducts.toDF()) {
       case (ip, (src, dest)) =>
         ip
           .alias("ohub")
           .join(products_sifu_limited.toDF(),
             ((ip("eanConsumerUnit") === products_sifu_limited("cuEanCode")) ||
               (ip("eanDistributionUnit") === products_sifu_limited("duEanCode")) ||
               (ip("code") === products_sifu_limited("productNumber")) ||
               (ip("code") === products_sifu_limited("productCode"))
               ) && (ip("countryCode") === upper(products_sifu_limited("country")))
             , "inner")
           .withColumn(s"sifu_$src", products_sifu_limited(src))
           .select("ohub.*", s"sifu_$src")
           .drop(s"$dest")
           .withColumnRenamed(s"sifu_$src", dest)
           .dropDuplicates("concatId")
     }.cache.as[Product]
    updatedWithSifu
  }

  override private[spark] def defaultConfig = UpdateProductsConfig()

  override private[spark] def configParser(): OptionParser[UpdateProductsConfig] =
    new scopt.OptionParser[UpdateProductsConfig]("Update Products") {
      head("merges products into an integrated products output file.", "1.0")
      opt[String]("currentIntegratedProducts") required() action { (x, c) ⇒
        c.copy(currentIntegratedProducts = x)
      } text "currentIntegratedProducts is a string property"
      opt[String]("productsSifuInputFile") required() action { (x, c) ⇒
        c.copy(productsSifuInputFile = x)
      } text "productsSifuInputFile is a string property"
      opt[String]("outputFile") required() action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"

      version("1.0")
      help("help") text "help text"
    }

  override def run(spark: SparkSession, config: UpdateProductsConfig, storage: Storage): Unit = {
    import spark.implicits._
    import org.apache.spark.sql.functions.{col, udf}
    implicit val sifuProductEncoder: Encoder[ProductSifu] = Encoders.product[ProductSifu]
    log.info(
      s"Updating products from [${config.productsSifuInputFile}] to [${config.outputFile}]"
    )
    val convert_to_timestamp = udf((s: String) =>
      parseDateTimeUnsafe()(s)
    )
    val products:Dataset[Product] = storage.readFromParquet[Product](config.currentIntegratedProducts)
    val products_sifu_raw = spark.read.parquet(config.productsSifuInputFile).cache()
    val products_sifu:Dataset[ProductSifu] = products_sifu_raw.select(col("code"), col("number"),
      col("country"), col("language"), col("name"), col("brandCode"),
      col("brandName"), col("subBrandCode"),
      col("subBrandName"), col("categoryCode"), col("categoryName"), col("subCategoryCode"),
      col("subCategoryName"), col("packagingCode"), col("packagingName"), col("cuEanCode"),
      col("duEanCode"), col("image1Id"), col("nutrientTypes"), col("nutrientValues"),
      col("allergens"), col("cuPriceInCents"), col("duPriceInCents"), col("productType"),
      col("packshotUrl"), col("packshotResizedUrl"), col("url"), col("loyaltyReward"),
      col("productName"), col("categoryCodes").getItem(0).alias("categoryCodes_1"),
      col("subCategoryCodes").getItem(0).alias("subCategoryCodes_1"),
      col("subCategoryNames").getItem(0).alias("subCategoryNames_1"),col("ingredients"),
      col("pictures").getItem("originalPicture").getItem("imageUrl").getItem(0).alias("extractedImageUrl"),
      col("pictures").getItem("originalPicture").getItem("imageUrl").getItem(1).alias("extractedPreviewImageUrl"),
      col("pictures").getItem("id").getItem(0).alias("extractedImageId"),col("dachClaimFooterTexts"),
      (convert_to_timestamp(col("lastModifiedDate")).alias("lastModifiedDate")),
      col("nutritionalData").getItem("nutrition").alias("extractedNutritionalType"),col("isUnileverProduct"),
      col("nutritionalData").getItem("per100gAsPrep").alias("extractedPer100gAsPrep"),col("convenienceLevel"),
      col("nutritionalData").getItem("per100gAsPrepOffPackUnitOfMeasurement").alias("per100gAsPrepOffPackUnitOfMeasurement"),
      col("productNumber"), col("productCode"), col("youtubeUrl"),col("itemType"),
      col("description"))
      .withColumn("extractedNutritionalValue",array_union($"extractedPer100gAsPrep",$"per100gAsPrepOffPackUnitOfMeasurement"))
      .cache
      .as[ProductSifu]
    val transformed = transform(spark,products_sifu, products)
    storage.writeToParquet(transformed, config.outputFile)
  }
}
