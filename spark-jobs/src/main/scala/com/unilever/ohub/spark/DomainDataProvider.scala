package com.unilever.ohub.spark

import java.util.Properties

import scala.io.Source
import com.unilever.ohub.spark.data._
import com.unilever.ohub.spark.sql.JoinType
import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.functions.col

trait DomainDataProvider {

  def countries: Map[String, CountryRecord]

  def countrySalesOrg: Map[String, CountrySalesOrg]

  def sourcePreferences: Map[String, Int]

  def channelMappings(): Dataset[ChannelMapping]
}

object DomainDataProvider {
  def apply(spark: SparkSession): DomainDataProvider =
    new InMemDomainDataProvider(spark)
}

class InMemDomainDataProvider(spark: SparkSession) extends DomainDataProvider with Serializable {

  import spark.implicits._

  override val countries: Map[String, CountryRecord] = {
    Source
      .fromInputStream(this.getClass.getResourceAsStream("/countries.csv"))
      .getLines()
      .toSeq
      .filter(_.nonEmpty)
      .map(_.split(","))
      .map(parts ⇒ CountryRecord(parts(6), parts(2), parts(10)))
      .filter(cr ⇒ cr.countryCode.nonEmpty && cr.countryName.nonEmpty && cr.currencyCode.nonEmpty)
      .map(cr ⇒ cr.countryCode -> cr)
      .toMap
  }

  override val countrySalesOrg: Map[String, CountrySalesOrg] = {
    Source
      .fromInputStream(this.getClass.getResourceAsStream("/country-codes-sales-org.csv"))
      .getLines()
      .toSeq
      .filter(_.nonEmpty)
      .map(_.split(","))
      .map(parts ⇒ CountrySalesOrg(parts(0), if (parts.length == 2) Option(parts(1)) else None))
      .filter(_.salesOrg.nonEmpty)
      .map(c ⇒ c.salesOrg.get -> c)
      .toMap
  }

  override val sourcePreferences: Map[String, Int] = {
    Source
      .fromInputStream(this.getClass.getResourceAsStream("/data-sources.csv"))
      .getLines()
      .toSeq
      .filter(_.nonEmpty)
      .drop(1)
      .map(_.split(","))
      .map(lineParts ⇒ lineParts(0) -> lineParts(1).toInt)
      .toMap
  }

  override def channelMappings(): Dataset[ChannelMapping] = {

    val channelMappingList = Source
      .fromInputStream(this.getClass.getClassLoader.getResourceAsStream("channel-mapping.csv"))
      .getLines()
      .toSeq
      .filter(_.nonEmpty)
      .toList
      .map(_.split(";"))
      .map(attributes ⇒ ChannelMappingRef(attributes(0), attributes(1), attributes(2), attributes(3), attributes(4)))

    val channelReferencesList = Source
      .fromInputStream(this.getClass.getClassLoader.getResourceAsStream("channel-references.csv"))
      .getLines()
      .toSeq
      .filter(_.nonEmpty)
      .toList
      .map(_.split(";"))
      .map(attributes ⇒ ChannelReferencesRef(attributes(0), attributes(1), attributes(2), attributes(3), attributes(4), attributes(5), attributes(6)))

    val channelMappingDF = spark
      .sparkContext
      .parallelize(channelMappingList)
      .toDF()

    val channelReferencesDF = spark
      .sparkContext
      .parallelize(channelReferencesList)
      .toDF()

    channelMappingDF
      .join(
        channelReferencesDF,
        col("channelReferenceFk") === col("channelReferenceId"),
        JoinType.Left
      )
      .select(
        $"countryCode" as "countryCode",
        $"originalChannel" as "originalChannel",
        $"localChannel" as "localChannel",
        $"channelUsage" as "channelUsage",
        $"socialCommercial" as "socialCommercial",
        $"strategicChannel" as "strategicChannel",
        $"globalChannel" as "globalChannel",
        $"globalSubChannel" as "globalSubChannel"
      )
      .as[ChannelMapping]
  }
}

class PostgresDomainDataProvider(spark: SparkSession, dbUrl: String, dbName: String, userName: String, userPassword: String) extends DomainDataProvider with Serializable {

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

  override def channelMappings(): Dataset[ChannelMapping] = {
    val channelMappingDF = readJdbcTable(spark, dbUrl, dbName, "channel_mapping", userName, userPassword)
    val channelReferencesDF = readJdbcTable(spark, dbUrl, dbName, "channel_references", userName, userPassword)

    channelMappingDF
      .join(
        channelReferencesDF,
        col("channel_reference_fk") === col("channel_reference_id"),
        JoinType.Left
      )
      .select(
        $"COUNTRY_CODE" as "countryCode",
        $"ORIGINAL_CHANNEL" as "originalChannel",
        $"LOCAL_CHANNEL" as "localChannel",
        $"CHANNEL_USAGE" as "channelUsage",
        $"SOCIAL_COMMERCIAL" as "socialCommercial",
        $"STRATEGIC_CHANNEL" as "strategicChannel",
        $"GLOBAL_CHANNEL" as "globalChannel",
        $"GLOBAL_SUBCHANNEL" as "globalSubChannel"
      )
      .as[ChannelMapping]
  }
}
