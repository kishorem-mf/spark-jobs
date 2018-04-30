package com.unilever.ohub.spark.tsv2parquet

import java.util.Properties

import com.unilever.ohub.spark.data.{ CountryRecord, CountrySalesOrg, SourcePreference }
import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

trait DomainDataProvider {

  def countries: Map[String, CountryRecord]

  def countrySalesOrg: Map[String, CountrySalesOrg]

  def sourcePreferences: Map[String, Int]
}

object DomainDataProvider {
  def apply(spark: SparkSession, dbUrl: String, dbName: String, userName: String, userPassword: String): DomainDataProvider =
    new PostgressDomainDataProvider(spark, dbUrl, dbName, userName, userPassword)
}

class PostgressDomainDataProvider(spark: SparkSession, dbUrl: String, dbName: String, userName: String, userPassword: String) extends DomainDataProvider with Serializable {
  import spark.implicits._

  // see also: http://spark.apache.org/docs/latest/sql-programming-guide.html#jdbc-to-other-databases
  private def readJdbcTable(
    spark: SparkSession,
    dbUrl: String,
    dbName: String,
    dbTable: String,
    userName: String,
    userPassword: String
  ): DataFrame = {
    val dbFullConnectionString = s"jdbc:postgresql://$dbUrl:5432/$dbName?ssl=true"

    val connectionProperties = new Properties
    connectionProperties.put("user", userName)
    connectionProperties.put("password", userPassword)

    spark
      .read
      .option(JDBCOptions.JDBC_DRIVER_CLASS, "org.postgresql.Driver")
      .jdbc(dbFullConnectionString, dbTable, connectionProperties)
  }

  override val countries: Map[String, CountryRecord] = {
    val countries = readJdbcTable(spark, dbUrl, dbName, "all_country_info", userName, userPassword)

    countries.select(
      $"ISO3166_1_Alpha_2" as "countryCode",
      $"official_name_en" as "countryName",
      $"ISO4217_currency_alphabetic_code" as "currencyCode"
    )
      .as[CountryRecord]
      .filter(cr ⇒ cr.countryCode.nonEmpty && cr.countryName.nonEmpty && cr.currencyCode.nonEmpty)
      .map(cr ⇒ cr.countryCode -> cr)
      .collect()
      .toMap
  }

  override val countrySalesOrg: Map[String, CountrySalesOrg] = {
    val countrySalesOrgs = readJdbcTable(spark, dbUrl, dbName, "country_codes", userName, userPassword)

    countrySalesOrgs.select(
      $"COUNTRY_CODE" as "countryCode",
      $"SALES_ORG" as "salesOrg"
    )
      .as[CountrySalesOrg]
      .filter(_.salesOrg.nonEmpty)
      .map(c ⇒ c.salesOrg.get -> c)
      .collect()
      .toMap
  }

  override val sourcePreferences: Map[String, Int] = {
    val sourcePreferences = readJdbcTable(spark, dbUrl, dbName, "data_sources", userName, userPassword)

    sourcePreferences.select(
      $"SOURCE" as "source",
      $"PRIORITY" as "priority"
    )
      .as[SourcePreference]
      .map(sp ⇒ sp.source -> sp.priority)
      .collect()
      .toMap
  }
}
