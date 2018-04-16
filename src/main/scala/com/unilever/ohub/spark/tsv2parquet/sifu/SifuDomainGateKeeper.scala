package com.unilever.ohub.spark.tsv2parquet.sifu

import java.net.{ URI, URL }

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.tsv2parquet.DomainGateKeeper
import io.circe._, io.circe.generic.auto._, io.circe.parser._, io.circe.syntax._
import org.apache.spark.sql.{ Dataset, SparkSession }

import scala.io.Source
import scala.util.{ Failure, Success, Try }

trait SifuDomainGateKeeper[T <: DomainEntity] extends DomainGateKeeper[T] {

  override protected[tsv2parquet] def partitionByValue: Seq[String] = Seq("countryCode")

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

  private val MAX = 1000000
  private val RANGE = 100

  protected[sifu] def sifuSelection: String

  override def read(spark: SparkSession, storage: Storage, input: String): Dataset[SifuProductResponse] = {
    import spark.implicits._

    val products = countryLanguageListEmakina
      .flatMap { case (country, lang) ⇒ getProductsFromApi(country, lang, sifuSelection, RANGE, MAX) }
    val res = spark
      .createDataset(products)
    res
  }

  private[sifu] def createSifuURL(
    countryCode: String,
    languageKey: String,
    sifuSelection: String,
    startIndex: Int,
    endIndex: Int
  ): Try[URL] = Try {
    val baseUri = new URI("https://sifu.unileversolutions.com:443")
    val typeParam = sifuSelection.toUpperCase().substring(0, sifuSelection.length - 1)
    baseUri
      .resolve(s"/$sifuSelection/$countryCode/$languageKey/$startIndex/$endIndex?type=$typeParam")
      .toURL
  }

  protected[sifu] def getResponseBodyString(url: URL): String = Source.fromURL(url).mkString

  private[sifu] def getProductsFromApi(
    countryCode: String,
    languageKey: String,
    sifuSelection: String,
    step: Int = 100,
    maxIterations: Int = 10
  ): Seq[SifuProductResponse] = {
    (0 to maxIterations by step).foldLeft(Seq.empty[SifuProductResponse]) {
      case (acc, i) ⇒
        val parsed: Seq[SifuProductResponse] = createSifuURL(countryCode, languageKey, sifuSelection, i, i + step) match {
          case Failure(e) ⇒ Seq.empty[SifuProductResponse]
          case Success(url) ⇒
            val body = getResponseBodyString(url)
            decode[Seq[SifuProductResponse]](body).right.getOrElse(List.empty)
        }
        acc ++ parsed
    }
  }
}
