package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.domain.entity.ContactPerson
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
      .cleanseMobileEmail
      .columnsNotNullAndNotEmpty(exactMatchColumnSeq)
      .filter($"emailAddress".isNull)
      .withColumn("priority", lit(Priority.SecondIntegrated.id))

    val deltaCPWithExactColumn = deltaContactPersons
      .cleanseMobileEmail
      .columnsNotNullAndNotEmpty(exactMatchColumnSeq)
      .filter($"emailAddress".isNull)
      .withColumn("priority",lit(Priority.ThirdDelta.id))

    val matchedCPOnExactColumn = referenceMatchedRecords
      .cleanseMobileEmail
      .withColumn("priority", lit(Priority.FirstEmail.id))
      .unionByName(integratedCPWithExactColumn)
      .unionByName(deltaCPWithExactColumn)
      .addOhubIdBasedOnColumnAndPriority(exactMatchColumn)
      .filter($"emailAddress".isNull)


    val matchedCPOnExactColumn1 = matchedCPOnExactColumn.drop(
      "priority","cleansedEmail", "cleansedMobile")
      .as[ContactPerson]

    val unMatchedCPIntegratedExactColumn = integratedContactPersons
      .join(matchedCPOnExactColumn, Seq("concatId"), JoinType.LeftAnti)
      .as[ContactPerson]


    val unMatchedCPDeltaExactColumn = deltaContactPersons
      .join(matchedCPOnExactColumn, Seq("concatId"), JoinType.LeftAnti)
      .as[ContactPerson]

    val matchedCPOnExactColumn2 = matchedCPOnExactColumn1
      .unionByName(referenceMatchedRecords)
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
      .cleanseMobileEmail
      .columnsNotNullAndNotEmpty(exactMatchColumnSeq)
      .withColumn("priority", lit(Priority.SecondIntegrated.id))

    val deltaCPWithExactColumn = deltaContactPersons
      .cleanseMobileEmail
      .columnsNotNullAndNotEmpty(exactMatchColumnSeq)
      .withColumn("priority",lit(Priority.ThirdDelta.id))

    val matchedCPOnExactColumn = referenceMatchedRecords
      .cleanseMobileEmail
      .withColumn("priority", lit(Priority.FirstEmail.id))
      .unionByName(integratedCPWithExactColumn)
      .unionByName(deltaCPWithExactColumn)
      .addOhubIdBasedOnColumnAndPriority(exactMatchColumn)

     val matchedCPOnExactColumnWithoutIntegRecords  =  matchedCPOnExactColumn
      .filter($"priority" =!= Priority.SecondIntegrated.id)
      .drop( "priority","cleansedEmail", "cleansedMobile")
      .as[ContactPerson]

    val unMatchedCPIntegratedExactColumn = integratedContactPersons
      .join(matchedCPOnExactColumn, Seq("concatId"), JoinType.LeftAnti)
      .as[ContactPerson]

    val unMatchedCPDeltaExactColumn = deltaContactPersons
      .join(matchedCPOnExactColumn, Seq("concatId"), JoinType.LeftAnti)
      .as[ContactPerson]

    (matchedCPOnExactColumnWithoutIntegRecords, unMatchedCPIntegratedExactColumn, unMatchedCPDeltaExactColumn)
  }

}

/**
  * This Enumerator indicates the highest priority for email
  */
object Priority extends Enumeration {
  type Priority = Value

  val FirstEmail      = Value("FirstEmail")
  val SecondIntegrated  = Value("SecondIntegrated")
  val ThirdDelta       = Value("ThirdDelta")
}
