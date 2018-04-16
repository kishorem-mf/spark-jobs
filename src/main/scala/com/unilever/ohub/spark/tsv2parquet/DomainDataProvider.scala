package com.unilever.ohub.spark.tsv2parquet

import com.unilever.ohub.spark.data.{ CountryRecord, CountrySalesOrg }
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.SparkSession

trait DomainDataProvider {

  def countries: Map[String, CountryRecord]

  def countrySalesOrg: Map[String, CountrySalesOrg]

  def sourcePreferences: Map[String, Int]

}

object DomainDataProvider {
  def apply(spark: SparkSession, storage: Storage): DomainDataProvider = {
    import spark.implicits._

    InMemDomainDataProvider(
      countries = storage.createCountries.map(c ⇒ c.countryCode -> c).collect().toMap,
      countrySalesOrg = storage.createCountriesSalesOrgMapping,
      sourcePreferences = storage.sourcePreference
    )
  }
}

case class InMemDomainDataProvider(
    countries: Map[String, CountryRecord],
    countrySalesOrg: Map[String, CountrySalesOrg],
    sourcePreferences: Map[String, Int])
  extends DomainDataProvider with Serializable
