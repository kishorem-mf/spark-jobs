package com.unilever.ohub.spark.matching

import com.unilever.ohub.spark.generic.{ FileSystems, StringFunctions }
import org.apache.log4j.{ LogManager, Logger }
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.{ Dataset, SparkSession }

case class DatasetAndCountry(ds: Dataset[_], countryCode: String)

case class RodrigoSchema(
  matched_string: String,
  country_code: String,
  id_i: String,
  id: String,
  similarity: Float,
  name_i: String,
  name_j: String
)

object OperatorExtraMatchingC extends App {
  implicit private val log: Logger = LogManager.getLogger(this.getClass)

  val (inputFile: String, outputFolder: String, rodrigoParquet: String) = FileSystems.getFileNames(
    args,
    "INPUT_FILE", "OUTPUT_FOLDER", "HELP_FILE"
  )

  val spark = SparkSession
    .builder()
    .appName("Operator matching")
    .getOrCreate()

  import spark.implicits._

  def createOperatorMatchGroupsPerCountry(
    outputFolder: String,
    countryCode: String,
    rodrigoDs: Dataset[RodrigoSchema]
  ): Dataset[_] = {
    val operatorsDF2 = spark.sql(
      """
        |select distinct country_code,OPERATOR_CONCAT_ID id,name,name_cleansed,zip_code,zip_code_cleansed,street,street_cleansed,city,city_cleansed,substring(name_cleansed,1,3) name_block,substring(street_cleansed,1,3) street_block
        |from operators1
      """.stripMargin)
      .filter($"country_code" === countryCode)

    operatorsDF2.createOrReplaceTempView("operators2")

    val rodrigoTableName = "rodrigo"
    val rodrigoEnriched = rodrigoDs
      .join(operatorsDF2, Seq("id", "country_code"))
      .filter($"country_code" === countryCode)

    rodrigoEnriched.createOrReplaceTempView(rodrigoTableName)

    val operatorsDF3 = spark.sql(
      s"""
         |select source.country_code,min(source.id) source_id,target.id target_id
         |from operators2 source
         |inner join $rodrigoTableName target
         | on source.id = target.id
         | and source.country_code = target.country_code
         |where 1 = 1
         | and similarity(source.name_cleansed,target.name_cleansed) > 0.85
         | and source.name_cleansed <= target.name_cleansed
         | and source.zip_code is null and target.zip_code is null and source.city_cleansed is null and target.city_cleansed is null and source.street_cleansed is null and target.street_cleansed is null
         |group by source.country_code,target.id
        """.stripMargin)
    operatorsDF3.createOrReplaceTempView("operators3")
    val operatorsDF4 = spark.sql(
      """
        |select source_id,target_id
        |from operators3
        |where source_id < target_id
      """.stripMargin)
    operatorsDF4.createOrReplaceTempView("operators4")
    val operatorsDF5 = spark.sql(
      s"""
         |select source.country_code,min(source.id) source_id,target.id target_id
         |from operators2 source
         |inner join $rodrigoTableName target
         | on source.zip_code_cleansed = target.zip_code_cleansed
         | and source.country_code = target.country_code
         |where 1 = 1
         | and similarity(source.name_cleansed,target.name_cleansed) > 0.8
         | and source.name_cleansed <= target.name_cleansed
         | and source.zip_code is not null and target.zip_code is not null and source.city_cleansed is null and target.city_cleansed is null and source.street_cleansed is null and target.street_cleansed is null
         |group by source.country_code,target.id
        """.stripMargin)
    operatorsDF5.createOrReplaceTempView("operators5")
    val operatorsDF6 = spark.sql(
      """
        |select source_id,target_id
        |from operators5
        |where source_id < target_id
      """.stripMargin)
    operatorsDF6.createOrReplaceTempView("operators6")
    val operatorsDF7 = spark.sql(
      s"""
         |select source.country_code,min(source.id) source_id,target.id target_id
         |from operators2 source
         |inner join $rodrigoTableName target
         | on source.country_code = target.country_code
         | and source.street_block = target.street_block
         | and source.city_cleansed = target.city_cleansed
         |where 1 = 1
         | and similarity(source.name_cleansed,target.name_cleansed) > 0.8
         | and similarity(source.street_cleansed,target.street_cleansed) > 0.8
         | and source.name_cleansed <= target.name_cleansed
         | and source.street_cleansed is not null and target.street_cleansed is not null
         |group by source.country_code,target.id
        """.stripMargin)
    operatorsDF7.createOrReplaceTempView("operators7")
    val operatorsDF8 = spark.sql(
      """
        |select source_id,target_id
        |from operators7
        |where source_id < target_id
      """.stripMargin)
    operatorsDF8.createOrReplaceTempView("operators8")
    val operatorsDF9 = spark.sql(
      s"""
         |select source.country_code,min(source.id) source_id,target.id target_id
         |from operators2 source
         |inner join $rodrigoTableName target
         | on source.country_code = target.country_code
         | and source.city_cleansed = target.city_cleansed
         |where 1 = 1
         | and similarity(source.name_cleansed,target.name_cleansed) > 0.8
         | and source.name_cleansed <= target.name_cleansed
         | and source.zip_code is null and target.zip_code is null and source.city_cleansed is not null and target.city_cleansed is not null and source.street_cleansed is null and target.street_cleansed is null
         |group by source.country_code,target.id
        """.stripMargin)
    operatorsDF9.createOrReplaceTempView("operators9")
    val operatorsDF10 = spark.sql(
      """
        |select source_id,target_id
        |from operators9
        |where source_id < target_id
      """.stripMargin)
    operatorsDF10.createOrReplaceTempView("operators10")
    val operatorsDF11 = operatorsDF4.union(operatorsDF6.union(operatorsDF8.union(operatorsDF10)))
    operatorsDF11.createOrReplaceTempView("operators11")
    val operatorsDF12 = spark.sql(
      """
        |select source_id,target_id
        |from operators11
        |where source_id < target_id
        |group by source_id,target_id
      """.stripMargin)
    operatorsDF12.createOrReplaceTempView("operators12")
    val operatorsDF13 = spark.sql(
      """
        |select distinct b.country_code,a.source_id,a.target_id,b.name source_name,b.zip_code source_zip,b.street source_street,b.city_cleansed source_city_cleansed
        |from operators12 a
        |inner join operators2 b
        | on a.source_id = b.id
      """.stripMargin)
    operatorsDF13.createOrReplaceTempView("operators13")
    spark.sql(
      """
        |select distinct a.country_code,a.source_id,a.target_id,a.source_name,b.name target_name,a.source_zip,b.zip_code target_zip,a.source_street,b.street target_street,a.source_city_cleansed,b.city_cleansed target_city_cleansed
        |from operators13 a
        |inner join operators2 b
        | on a.target_id = b.id
        |order by a.source_id
      """.stripMargin)
  }

  spark.sqlContext.udf.register("SIMILARITY", (s1: String, s2: String) => {
    StringFunctions.getFastSimilarity(s1.toCharArray, s2.toCharArray)
  })

  val operatorsDF1 = spark.read.parquet(inputFile)
  operatorsDF1.createOrReplaceTempView("operators1")

//  val countryList = operatorsDF1
//    .select("country_code")
//    .groupBy("country_code")
//    .count()
//    .orderBy("count")
//    .collect()
//    .map(row => row(0).toString)
//    .toList
  val countryList = List("US")

  val rodrigo = spark.read.parquet(rodrigoParquet)
    .withColumnRenamed("id_j", "id")
    .as[RodrigoSchema]

  countryList
    .map(countryCode => {
      DatasetAndCountry(createOperatorMatchGroupsPerCountry(outputFolder, countryCode, rodrigo), countryCode)
    })
    .foreach(x =>
      x.ds.write.mode(Overwrite).parquet(s"$outputFolder/${x.countryCode}.parquet")
    )

  log.info("Done")
}
