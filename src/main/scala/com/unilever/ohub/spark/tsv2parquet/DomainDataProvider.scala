package com.unilever.ohub.spark.tsv2parquet

import com.unilever.ohub.spark.data.CountryRecord
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.SparkSession

trait DomainDataProvider {

  def countries: Map[String, CountryRecord]

  def sourcePreferences: Map[String, Int]
}

object DomainDataProvider {
  def apply(spark: SparkSession, storage: Storage): DomainDataProvider = {
    import spark.implicits._

    InMemDomainDataProvider(
      countries = storage.createCountries.map(c ⇒ c.countryCode -> c).collect().toMap,
      sourcePreferences = storage.sourcePreference
    )
  }
}

case class InMemDomainDataProvider(countries: Map[String, CountryRecord], sourcePreferences: Map[String, Int]) extends DomainDataProvider with Serializable
