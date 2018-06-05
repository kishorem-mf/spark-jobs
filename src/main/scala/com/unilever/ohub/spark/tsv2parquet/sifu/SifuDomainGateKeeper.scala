package com.unilever.ohub.spark.tsv2parquet.sifu

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.tsv2parquet.DomainGateKeeper
import com.unilever.ohub.spark.tsv2parquet.DomainGateKeeper.DomainConfig
import org.apache.spark.sql.{ Dataset, SparkSession }

trait SifuDomainGateKeeper[T <: DomainEntity] extends DomainGateKeeper[T, SifuProductResponse] {

  override protected[tsv2parquet] def partitionByValue: Seq[String] = Seq()

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
  private[sifu] val countryAndLanguages = countryListEmakina zip languageListEmakina.toList // todo fix order

  private val PAGE_SIZE = 100
  private val PAGES = 10

  protected[sifu] def sifuSelection: String

  protected[sifu] def sifuDataProvider: SifuDataProvider

  override def read(spark: SparkSession, storage: Storage, config: DomainConfig): Dataset[SifuProductResponse] = {
    read(spark, storage, config, sifuDataProvider)
  }

  protected[sifu] def read(spark: SparkSession, storage: Storage, config: DomainConfig, sifuDataProvider: SifuDataProvider): Dataset[SifuProductResponse] = {
    import spark.implicits._

    val jsons = countryAndLanguages
      .flatMap { case (country, lang) ⇒ sifuDataProvider.productsFromApi(country, lang, sifuSelection, PAGE_SIZE, PAGES) }
    spark
      .createDataset(jsons)
  }
}
