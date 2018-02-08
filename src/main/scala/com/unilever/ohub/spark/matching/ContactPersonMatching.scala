package com.unilever.ohub.spark.matching

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }

case class ContactPerson(
  country_code: String,
  source: String,
  ref_contact_person_id: String,
  first_name: String,
  last_name: String,
  both_names_cleansed: String,
  zip_code: String,
  zip_code_cleansed: String,
  street: String ,
  street_cleansed: String,
  city: String,
  city_cleansed: String,
  email_address: Option[String],
  mobile_phone_number: Option[String]
) {
  val id: String = country_code + '~' + source + '~' + ref_contact_person_id
  val name_block: String = both_names_cleansed.substring(1, 3)
  val street_block: String = street_cleansed.substring(1, 3)
}

object ContactPersonMatching extends SparkJob {
  def transform(spark: SparkSession, contactPersons: Dataset[ContactPerson]): Dataset[ContactPerson] = {
    import spark.implicits._

    contactPersons
      .filter(cpn => cpn.email_address.nonEmpty || cpn.mobile_phone_number.nonEmpty)
      .groupByKey(cpn => cpn.email_address.getOrElse("") + cpn.mobile_phone_number.getOrElse(""))
      .flatMapGroups {
        case (_, contactPersonsIt) => contactPersonsIt
      }
  }

  override val neededFilePaths = Array("INPUT_FILE", "OUTPUT_FILE")

  override def run(spark: SparkSession, filePaths: Product, storage: Storage): Unit = {
    import spark.implicits._

    val (inputFile: String, outputFile: String) = filePaths

    log.info(s"Generating parquet from [$inputFile] to [$outputFile]")

    val contactPersons = storage
      .readFromParquet[ContactPerson](inputFile)

    val transformed = transform(spark, contactPersons)

    storage
      .writeToParquet(transformed, outputFile, partitionBy = "COUNTRY_CODE")
  }
}
