package com.unilever.ohub.spark.recommandation

import java.net.{ URI, URL }

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }

import scala.io.Source
import scala.util.{ Failure, Success, Try }

case class SifuSelection(INFO_TYPE: String)

object SifuConverter extends SparkJob {
  private val countryListEmakina = Array("AE", "AE", "AF", "AF", "AR", "AR", "AT", "AT", "AT", "AT", "AU", "AU",
    "AU", "AU", "AU", "AU", "AU", "AU", "AU", "AZ", "AZ", "BE", "BE", "BG", "BH", "BH", "BR", "BR", "CA",
    "CA", "CH", "CH", "CL", "CN", "CN", "CN", "CN", "CN", "CN", "CN", "CN", "CN", "CN", "CN", "CN", "CN",
    "CN", "CN", "CN", "CN", "CN", "CN", "CN", "CN", "CN", "CO", "CR", "CX", "CZ", "DE", "DE", "DE", "DK",
    "EE", "EG", "EG", "EG", "ES", "ET", "FI", "FI", "FR", "FR", "GB", "GR", "GT", "HN", "HU", "ID", "ID",
    "ID", "ID", "ID", "ID", "ID", "ID", "ID", "ID", "IE", "IE", "IE", "IE", "IE", "IE", "IE", "IE", "IE",
    "IE", "IE", "IE", "IL", "IN", "IN", "IT", "JO", "JO", "KR", "KR", "KR", "KR", "KR", "KR", "KR", "KR",
    "KR", "KW", "KW", "LB", "LB", "LK", "LK", "LK", "LT", "LT", "LT", "LU", "LV", "LV", "MM", "MV", "MX",
    "MX", "MX", "MX", "MX", "MX", "MX", "MX", "MX", "MX", "MX", "MX", "MX", "MX", "MX", "MX", "MX", "MX",
    "MX", "MX", "MX", "MY", "MY", "MY", "NI", "NI", "NL", "NL", "NO", "NZ", "NZ", "NZ", "NZ", "NZ", "NZ",
    "NZ", "OM", "OM", "PA", "PH", "PH", "PH", "PH", "PH", "PH", "PK", "PK", "PK", "PL", "PL", "PT", "PY",
    "QA", "QA", "RO", "RU", "SA", "SA", "SE", "SE", "SG", "SG", "SG", "SG", "SG", "SG", "SK", "SV", "TH",
    "TH", "TH", "TH", "TR", "TR", "US", "US", "US", "US", "US", "US", "US", "US", "US", "US", "US", "US",
    "US", "US", "US", "US", "US", "US", "US", "US", "VN", "VN", "VN", "ZA", "ZA")
  private val languageListEmakina = Array("ar", "en", "de", "zh", "es", "fi", "ab", "de", "en", "tr", "da", "de",
    "en", "es", "fi", "he", "pt", "ru", "tr", "az", "tr", "fr", "nl", "bg", "ar", "en", "fi", "pt", "en",
    "fr", "de", "fr", "es", "ar", "bg", "cs", "da", "de", "en", "es", "fi", "fr", "he", "hu", "it", "lt",
    "lv", "nl", "no", "pl", "pt", "ru", "sv", "tr", "zh", "es", "es", "en", "cs", "de", "en", "fr", "da",
    "et", "ar", "de", "en", "es", "et", "de", "fi", "fr", "nl", "en", "el", "es", "es", "hu", "de", "en",
    "es", "fi", "he", "id", "no", "pt", "tr", "zh", "da", "de", "en", "es", "fi", "he", "id", "pt", "sv",
    "th", "tr", "zh", "he", "en", "zh", "it", "ar", "en", "da", "de", "en", "es", "fi", "ko", "pl", "pt",
    "tr", "ar", "en", "ar", "en", "en", "si", "zh", "et", "lt", "lv", "fr", "lt", "lv", "my", "en", "bg",
    "da", "de", "en", "es", "fi", "fr", "he", "hu", "id", "it", "lv", "nl", "no", "pl", "pt", "ru", "sv",
    "th", "tr", "zh", "en", "ms", "zh", "en", "es", "fr", "nl", "no", "da", "de", "en", "es", "fi", "he",
    "pt", "ar", "en", "es", "da", "de", "en", "fi", "tl", "zh", "en", "ur", "zh", "en", "pl", "pt", "es",
    "ar", "en", "ro", "ru", "ar", "en", "sv", "zh", "en", "es", "fi", "nl", "tr", "zh", "sk", "es", "de",
    "en", "es", "th", "nl", "tr", "bg", "da", "de", "en", "es", "fi", "fr", "he", "hu", "id", "it", "ko",
    "nl", "no", "pl", "pt", "ru", "sv", "tr", "zh", "en", "vi", "zh", "af", "en")
  private val countryLanguageListEmakina = countryListEmakina zip languageListEmakina.toList
  private val PRODUCTS: SifuSelection = SifuSelection("products")
  private val RECIPES: SifuSelection = SifuSelection("recipes")
  private val MAX = 1000000
  private val RANGE = 100

  def createSifuURL(
    countryCode: String,
    languageKey: String,
    sifuSelection: SifuSelection,
    startIndex: Int,
    endIndex: Int
  ): Try[URL] = Try {
    val baseUri = new URI("https://sifu.unileversolutions.com:443")
    val typeParam = sifuSelection.INFO_TYPE.toUpperCase().substring(0, sifuSelection.INFO_TYPE.length - 1)
    baseUri
      .resolve(s"/${sifuSelection.INFO_TYPE}/$countryCode/$languageKey/$startIndex/$endIndex?type=$typeParam")
      .toURL
  }

  def getResponseBodyString(url: URL): String = Source.fromURL(url).mkString

  def getConcatenatedJsonString(
    countryCode: String,
    languageKey: String,
    sifuSelection: SifuSelection,
    range: Int,
    maxIterations: Int
  ): String = {
    def inner(index: Int, acc: String = ""): String = {
      if (index >= maxIterations) acc
      else {
        createSifuURL(countryCode, languageKey, sifuSelection, index, index + range) match {
          case Failure(e)   => log.error("Failed to create SIFU URL", e); ""
          case Success(url) =>
            val body = getResponseBodyString(url)

            if (body.nonEmpty) inner(index + 1, acc + body)
            else acc
        }
      }
    }

    inner(0)
  }

  def transform(spark: SparkSession, jsonStrings: Array[String]): Dataset[String] = {
    import spark.implicits._
    spark.sparkContext.parallelize(jsonStrings).toDS()
  }

  override val neededFilePaths = Array("PRODUCTS_FILE", "RECIPES_FILE")

  override def run(spark: SparkSession, filePaths: Product, storage: Storage): Unit = {
    val (productsFile, recipesFile) = filePaths

    log.info(s"Generating SIFU_PRODUCTS parquet [$productsFile] and SIFU_RECIPES parquet [$recipesFile]")

    val productsJsonStrings = countryLanguageListEmakina
      .map { case (country, lang) => getConcatenatedJsonString(country, lang, PRODUCTS, RANGE, MAX) }
      .filter(_.nonEmpty)

    val recipesJsonStrings = countryLanguageListEmakina
      .map { case (country, lang) => getConcatenatedJsonString(country, lang, RECIPES, RANGE, MAX) }
      .filter(_.nonEmpty)

    productsJsonStrings.exists(_.nonEmpty) -> recipesJsonStrings.exists(_.nonEmpty) match {
      case (true, true) => log.info("Products and recipes found")
      case (true, _)    => log.warn("Products found, recipes not found")
      case (_, false)   => log.warn("Recipes found, products not found")
      case _            =>
        log.error("No products or recipes found, terminating...")
        sys.exit(1)
    }

    val products = transform(spark, productsJsonStrings)
    val recipes = transform(spark, recipesJsonStrings)

    //  TODO create selection "queries" based on raw parquet needed for REC-O
    storage
      .writeToParquet(products, productsFile)
    storage
      .writeToParquet(recipes, recipesFile)
  }
}
