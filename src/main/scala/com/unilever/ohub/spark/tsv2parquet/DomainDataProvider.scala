package com.unilever.ohub.spark.tsv2parquet

import java.io.{ File, FileOutputStream }

import com.unilever.ohub.spark.data.{ CountryRecord, CountrySalesOrg }
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }

import scala.io.Source

trait DomainDataProvider {

  def countries: Map[String, CountryRecord]

  def countrySalesOrg: Map[String, CountrySalesOrg]

  def sourcePreferences: Map[String, Int]
}

object DomainDataProvider {
  def apply(spark: SparkSession, storage: Storage): DomainDataProvider = {
    import spark.implicits._

    def createCsvSource(fn: String): Unit = {
      val in = this.getClass.getResourceAsStream(s"/$fn")
      val out = new FileOutputStream(new File(fn))

      Iterator
        .continually(in.read)
        .takeWhile(_ != -1)
        .foreach(b ⇒ out.write(b))

      out.close()
      in.close()
    }

    def createCountries: Dataset[CountryRecord] = {
      val file = "country_codes.csv"
      createCsvSource(file)

      storage.readFromCsv(file, fieldSeparator = ",")
        .select(
          $"ISO3166_1_Alpha_2" as "countryCode",
          $"official_name_en" as "countryName",
          $"ISO4217_currency_alphabetic_code" as "currencyCode"
        )
        .where($"countryCode".isNotNull and $"countryName".isNotNull and $"currencyCode".isNotNull)
        .as[CountryRecord]
    }

    def createCountriesSalesOrgMapping: Map[String, CountrySalesOrg] = {
      val file = "country_codes_sales_org.csv"
      createCsvSource(file)

      storage.readFromCsv(file, fieldSeparator = ",")
        .as[CountrySalesOrg]
        .collect()
        .filter(_.salesOrg.nonEmpty)
        .map(c ⇒ c.salesOrg.get -> c)
        .toMap
    }

    def sourcePreference: Map[String, Int] = {
      Source
        .fromInputStream(this.getClass.getResourceAsStream("/source_preference.tsv"))
        .getLines()
        .toSeq
        .filter(_.nonEmpty)
        .filterNot(_.equals("SOURCE\tPRIORITY"))
        .map(_.split("\t"))
        .map(lineParts ⇒ lineParts(0) -> lineParts(1).toInt)
        .toMap
    }

    InMemDomainDataProvider(
      countries = createCountries.map(c ⇒ c.countryCode -> c).collect().toMap,
      countrySalesOrg = createCountriesSalesOrgMapping,
      sourcePreferences = sourcePreference
    )
  }
}

case class InMemDomainDataProvider(
    countries: Map[String, CountryRecord],
    countrySalesOrg: Map[String, CountrySalesOrg],
    sourcePreferences: Map[String, Int])
  extends DomainDataProvider with Serializable
