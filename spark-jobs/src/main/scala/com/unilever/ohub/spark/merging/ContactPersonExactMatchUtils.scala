package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.ingest.EmptyParquetWriter
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import com.unilever.ohub.spark.merging.DataFrameHelpers._
import com.unilever.ohub.spark.sql.JoinType

object ContactPersonExactMatchUtils {


  def determineExactMatchOnEmailAndMobile(
       integratedContactPersons: Dataset[ContactPerson],
       dailyDeltaContactPersons: Dataset[ContactPerson])(implicit spark: SparkSession): Dataset[ContactPerson] = {

    import spark.implicits._

    val referenceEmptyRecords = spark.createDataset[ContactPerson](Seq[ContactPerson]())

    val (matchedCPOnEmail, unMatchedCPIntegratedOnEmail,  unMatchedCPDeltaOnEmail) = getMatchedAndUnmatchedEmailAndMobile(
      "cleansedEmail",
      referenceEmptyRecords,
      integratedContactPersons,
      dailyDeltaContactPersons)

    println("matchedCPOnEmail")
    matchedCPOnEmail.select("concatId", "ohubId","emailAddress","mobileNumber").show()
    println("===============================================================")

    println("unMatchedCPIntegratedOnEmail")
    unMatchedCPIntegratedOnEmail.select("concatId", "ohubId","emailAddress","mobileNumber").show()
    println("===============================================================")

    println("unMatchedCPDeltaOnEmail")
    unMatchedCPDeltaOnEmail.select("concatId", "ohubId","emailAddress","mobileNumber").show()



    val (matchedCPOnMobileAndEmail, unMatchedIntegratedCPOnMobile, unMatchedDeltaCPOnMobile) = getMatchedAndUnmatchedEmailAndMobile(
      "cleansedMobile",
      matchedCPOnEmail, //.dropDuplicates("mobileNumber"),
      unMatchedCPIntegratedOnEmail,
      unMatchedCPDeltaOnEmail)

    println("matchedCPOnMobile")
    matchedCPOnMobileAndEmail.select("concatId", "ohubId","emailAddress","mobileNumber").show()
    println("===============================================================")
    println("unMatchedCPOnMobile")
    unMatchedIntegratedCPOnMobile.select("concatId", "ohubId","emailAddress","mobileNumber").show()


    matchedCPOnMobileAndEmail
  }



  private def getMatchedAndUnmatchedEmailAndMobile(
      exactMatchColumn: String,
      referenceMatchedRecords: Dataset[ContactPerson],
      integratedContactPersons: Dataset[ContactPerson],
      deltaContactPersons: Dataset[ContactPerson])(implicit spark: SparkSession) = {

    import spark.implicits._

    val exactMatchColumnSeq = Seq(exactMatchColumn).map(col)

    val integratedCPWithExactColumn = integratedContactPersons
      .cleansMobileEmail
      .columnsNotNullAndNotEmpty(exactMatchColumnSeq)
      .withColumn("priority", lit(2))
      //.withColumn("inDelta", lit(false))

    val deltaCPWithExactColumn = deltaContactPersons
      .cleansMobileEmail
      .columnsNotNullAndNotEmpty(exactMatchColumnSeq)
      .withColumn("priority",lit(3))
      //.withColumn("inDelta", lit(true))
    deltaCPWithExactColumn.show()
    integratedCPWithExactColumn.show()
    val matchedCPOnExactColumn = referenceMatchedRecords
      .cleansMobileEmail
      .withColumn("priority", lit(1))
      .union(integratedCPWithExactColumn)
      .union(deltaCPWithExactColumn)
      .addOhubIdBasedOnColumnAndPriority(exactMatchColumn)
      //.selectLatestRecord


    println("matchedCPOnExactColumn")
    matchedCPOnExactColumn.select("concatId", "ohubId","emailAddress","mobileNumber","priority","cleansedEmail", "cleansedMobile").show()
    println("===============================================================")


     val matchedCPOnExactColumn1 =  matchedCPOnExactColumn.drop( "priority","cleansedEmail", "cleansedMobile")
      .as[ContactPerson]
    matchedCPOnExactColumn1.show()
    val unMatchedCPIntegratedExactColumn = integratedContactPersons
      .join(matchedCPOnExactColumn, Seq("concatId"), JoinType.LeftAnti)
      .as[ContactPerson]

    unMatchedCPIntegratedExactColumn.show()

    val unMatchedCPDeltaExactColumn = deltaContactPersons
      .join(matchedCPOnExactColumn, Seq("concatId"), JoinType.LeftAnti)
      .as[ContactPerson]

    unMatchedCPDeltaExactColumn.show()

    (matchedCPOnExactColumn1, unMatchedCPIntegratedExactColumn, unMatchedCPDeltaExactColumn)
  }
/*

  def cleansMobileEmail(contactPersonsDS: Dataset[ContactPerson])(implicit spark: SparkSession): Dataset[ContactPerson] = {
    import spark.implicits._

    contactPersonsDS
      .withColumn("cleansedEmail", trim(lower($"emailAddress")))
      .withColumn("cleansedMobile", regexp_replace($"mobileNumber", "(^0+)|([\\+\\-\\s])", ""))
      .as[ContactPerson]
  }
*/


}
