package com.unilever.ohub.spark.tsv2parquet

import com.unilever.ohub.spark.data.CountryRecord
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.SparkSession

trait DomainDataProvider {

  def countries: Map[String, CountryRecord]

  def sourcePreferences: Map[String, Int]
}

class SparkDomainDataProvider(spark: SparkSession, storage: Storage) extends DomainDataProvider {
  import spark.implicits._

  override lazy val countries: Map[String, CountryRecord] = storage.createCountries.map(c ⇒ c.countryCode -> c).collect().toMap

  override lazy val sourcePreferences: Map[String, Int] = storage.sourcePreference
}
