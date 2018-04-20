package com.unilever.ohub.spark.tsv2parquet

import com.unilever.ohub.spark.data.{ CountryRecord, CountrySalesOrg }
import org.apache.spark.sql.SparkSession

import scala.io.Source

trait DomainDataProvider {

  def countries: Map[String, CountryRecord]

  def countrySalesOrg: Map[String, CountrySalesOrg]

  def sourcePreferences: Map[String, Int]
}

object DomainDataProvider {
  def apply(spark: SparkSession): DomainDataProvider = {

    def countryMapping: Map[String, CountryRecord] = {
      Source
        .fromInputStream(this.getClass.getResourceAsStream("/country_codes.csv"))
        .getLines()
        .toSeq
        .filter(_.nonEmpty)
        .map(_.split(","))
        .map(parts ⇒ CountryRecord(parts(6), parts(2), parts(10)))
        .filter(cr ⇒ cr.countryCode.nonEmpty && cr.countryName.nonEmpty && cr.currencyCode.nonEmpty)
        .map(cr ⇒ cr.countryCode -> cr)
        .toMap
    }

    def countriesSalesOrgMapping: Map[String, CountrySalesOrg] = {
      Source
        .fromInputStream(this.getClass.getResourceAsStream("/country_codes_sales_org.csv"))
        .getLines()
        .toSeq
        .filter(_.nonEmpty)
        .map(_.split(","))
        .map(parts ⇒ CountrySalesOrg(parts(0), if (parts.length == 2) Option(parts(1)) else None))
        .filter(_.salesOrg.nonEmpty)
        .map(c ⇒ c.salesOrg.get -> c)
        .toMap
    }

    def sourcePreferenceMapping: Map[String, Int] = {
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
      countries = countryMapping,
      countrySalesOrg = countriesSalesOrgMapping,
      sourcePreferences = sourcePreferenceMapping
    )
  }
}

case class InMemDomainDataProvider(
    countries: Map[String, CountryRecord],
    countrySalesOrg: Map[String, CountrySalesOrg],
    sourcePreferences: Map[String, Int])
  extends DomainDataProvider with Serializable
