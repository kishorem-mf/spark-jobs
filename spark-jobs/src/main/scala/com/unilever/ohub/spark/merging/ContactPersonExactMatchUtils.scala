package com.unilever.ohub.spark.merging

import java.util.Optional

import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.ingest.EmptyParquetWriter
import org.apache.spark.sql.{Column, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import com.unilever.ohub.spark.merging.DataFrameHelpers._
import com.unilever.ohub.spark.sql.JoinType

object ContactPersonExactMatchUtils {


  def determineExactMatchOnEmailAndMobile(
       integratedContactPersons: Dataset[ContactPerson],
       dailyDeltaContactPersons: Dataset[ContactPerson])(implicit spark: SparkSession): Dataset[ContactPerson] = {

    import spark.implicits._

    val referenceEmptyRecords = spark.createDataset[ContactPerson](Seq[ContactPerson]())

    val (matchedCPOnEmail, unMatchedCPIntegratedOnEmail,  unMatchedCPDeltaOnEmail) = getMatchedAndUnmatchedEmail(
      "cleansedEmail",
      referenceEmptyRecords,
      integratedContactPersons,
      dailyDeltaContactPersons)

   val (matchedCPOnMobileAndEmail, unMatchedIntegratedCPOnMobile, unMatchedDeltaCPOnMobile) = getMatchedAndUnmatchedMobile(
      "cleansedMobile",
      matchedCPOnEmail,
      unMatchedCPIntegratedOnEmail,
      unMatchedCPDeltaOnEmail)

    matchedCPOnMobileAndEmail
  }

  private def getMatchedAndUnmatchedMobile(
                                           exactMatchColumn: String,
                                           referenceMatchedRecords: Dataset[ContactPerson],
                                           integratedContactPersons: Dataset[ContactPerson],
                                           deltaContactPersons: Dataset[ContactPerson])(implicit spark: SparkSession) = {

    import spark.implicits._

    val exactMatchColumnSeq = Seq(exactMatchColumn).map(col)

    val integratedCPWithExactColumn = integratedContactPersons
      .cleansMobileEmail
      .columnsNotNullAndNotEmpty(exactMatchColumnSeq)
      .filter($"emailAddress".isNull)
      .withColumn("priority", lit(2))
    //.withColumn("inDelta", lit(false))

    val deltaCPWithExactColumn = deltaContactPersons
      .cleansMobileEmail
      .columnsNotNullAndNotEmpty(exactMatchColumnSeq)
      .filter($"emailAddress".isNull)
      .withColumn("priority",lit(3))
    //.withColumn("inDelta", lit(true))

    val matchedCPOnExactColumn = referenceMatchedRecords
      .cleansMobileEmail
      .withColumn("priority", lit(1))
      .unionByName(integratedCPWithExactColumn)
      .unionByName(deltaCPWithExactColumn)
      .addOhubIdBasedOnColumnAndPriority1(exactMatchColumn)
      .filter($"emailAddress".isNull)
      .as[ContactPerson]


    val matchedCPOnExactColumn1 =  matchedCPOnExactColumn.drop( "priority","cleansedEmail", "cleansedMobile")
      .as[ContactPerson]

    val unMatchedCPIntegratedExactColumn = integratedContactPersons
      .join(matchedCPOnExactColumn, Seq("concatId"), JoinType.LeftAnti)
      .as[ContactPerson]


    val unMatchedCPDeltaExactColumn = deltaContactPersons
      .join(matchedCPOnExactColumn, Seq("concatId"), JoinType.LeftAnti)
      .as[ContactPerson]

    val matchedCPOnExactColumn2 = matchedCPOnExactColumn1
      .unionByName(referenceMatchedRecords)
      .dropDuplicates("concatId")
      .as[ContactPerson]

    (matchedCPOnExactColumn2, unMatchedCPIntegratedExactColumn, unMatchedCPDeltaExactColumn)
  }


  private def getMatchedAndUnmatchedEmail(
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

    val matchedCPOnExactColumn = referenceMatchedRecords
      .cleansMobileEmail
      .withColumn("priority", lit(1))
      .unionByName(integratedCPWithExactColumn)
      .unionByName(deltaCPWithExactColumn)
      .addOhubIdBasedOnColumnAndPriority(exactMatchColumn)
      .as[ContactPerson]

     val matchedCPOnExactColumn1 =  matchedCPOnExactColumn.drop( "priority","cleansedEmail", "cleansedMobile")
      .as[ContactPerson]

    val unMatchedCPIntegratedExactColumn = integratedContactPersons
      .join(matchedCPOnExactColumn, Seq("concatId"), JoinType.LeftAnti)
      .as[ContactPerson]

    val unMatchedCPDeltaExactColumn = deltaContactPersons
      .join(matchedCPOnExactColumn, Seq("concatId"), JoinType.LeftAnti)
      .as[ContactPerson]

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
