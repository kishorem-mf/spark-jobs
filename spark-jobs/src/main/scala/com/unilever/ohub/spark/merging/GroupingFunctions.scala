package com.unilever.ohub.spark.merging

import java.util.UUID

import com.unilever.ohub.spark.SparkJobConfig
import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.merging.DataFrameHelpers._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._

case class ExactMatchIngestedWithDbConfig(
    integratedInputFile: String = "path-to-integrated-input-file",
    deltaInputFile: String = "path-to-delta-input-file",
    matchedExactOutputFile: String = "path-to-matched-exact-output-file",
    unmatchedIntegratedOutputFile: String = "path-to-unmatched-integrated-output-file",
    unmatchedDeltaOutputFile: String = "path-to-unmatched-delta-output-file"
) extends SparkJobConfig

trait GroupingFunctions {

  val createOhubIdUdf: UserDefinedFunction = udf((d: Double) ⇒ UUID.nameUUIDFromBytes(d.toString.getBytes).toString)

  def matchColumns[T <: DomainEntity: Encoder](
    integrated: Dataset[T],
    delta: Dataset[T],
    groupingColumns: Seq[String],
    notNullColoumns: Seq[String])(implicit spark: SparkSession): Dataset[T] = {
    import spark.implicits._

    val sameColumns = groupingColumns.map(col)
    val notNullCheckColumns = notNullColoumns.map(col)

    val integratedWithExact = integrated
      .columnsNotNullAndNotEmpty(notNullCheckColumns)
      .concatenateColumns("group", sameColumns)
      .withColumn("inDelta", lit(false))

    val newWithExact = delta
      .columnsNotNullAndNotEmpty(notNullCheckColumns)
      .concatenateColumns("group", sameColumns)
      .withColumn("inDelta", lit(true))

    integratedWithExact
      .union(newWithExact)
      .addOhubId
      .drop("group")
      .selectLatestRecord
      .drop("inDelta")
      .as[T]
  }
}

object DataFrameHelpers extends GroupingFunctions {

  implicit class Helpers(df: Dataset[_]) {

    /**
     * Concatenate all the specified
     * @param cols and create new column with name
     * @param name
     * @param convertToLowerCase is used to make convert all column values to lower case. By default it is false
     */
    def concatenateColumns(name: String, cols: Seq[Column], convertToLowerCase: Boolean = false)(implicit spark: SparkSession): Dataset[_] = {
      df.withColumn(name, concat(cols.map(c ⇒
        when(c.isNull, "").otherwise(
          if (!convertToLowerCase) c
          else trim(lower(c)))): _*))
    }

    def addOhubId(implicit spark: SparkSession): Dataset[_] = {
      import spark.implicits._
      val w1 = Window.partitionBy($"group").orderBy($"ohubId".desc_nulls_last)
      df.withColumn("ohubId", first($"ohubId").over(w1)) // preserve ohubId

        // the next two lines will select a deterministic random ohubId
        .withColumn("rand", concat(monotonically_increasing_id(), rand()))
        .withColumn("ohubId", when('ohubId.isNull, createOhubIdUdf($"rand")).otherwise('ohubId))

        .withColumn("ohubId", first('ohubId).over(w1)) // make sure the whole group gets the same ohubId
        .drop("rand")

    }

    def addOhubIdBasedOnColumnAndPriority(exactMatchColumn: String)(implicit spark: SparkSession): Dataset[_] = {

      import spark.implicits._
      val w1 = Window.partitionBy(col(exactMatchColumn)).orderBy($"priority", $"ohubId".desc_nulls_last)
      //Date Created can be used instead of priority
      df.withColumn("ohubId", first($"ohubId").over(w1)) // preserve ohubId

        // the next two lines will select a deterministic random ohubId
        .withColumn("rand", concat(monotonically_increasing_id(), rand()))
        .withColumn("ohubId", when('ohubId.isNull, createOhubIdUdf($"rand")).otherwise('ohubId))
        .withColumn("ohubId", first('ohubId).over(w1)) // make sure the whole group gets the same ohubId
        .drop("rand")
    }

    def cleanseMobileEmail(implicit spark: SparkSession): Dataset[_] = {
      import spark.implicits._

      df
        .withColumn("cleansedEmail", trim(lower($"emailAddress")))
        .withColumn("cleansedMobile", regexp_replace($"mobileNumber", "(^0+)|([\\+\\-\\s])", ""))
    }

    def columnsNotNullAndNotEmpty(col: Column, cols: Column*): Dataset[_] = {
      columnsNotNullAndNotEmpty(col +: cols)
    }

    /**
     * Keep rows where all columns are not null and not empty
     */
    def columnsNotNullAndNotEmpty(cols: Seq[Column]): Dataset[_] = {
      def notNullOrEmpty(col: Column): Column = col.isNotNull and col.notEqual("")

      df.filter(cols.map(c ⇒ notNullOrEmpty(c)).reduce(_ and _))
    }

    /**
      * Keep rows where all columns are not null and not empty
      */
    def columnsCondition(cols: Seq[Column], colCondition:Column): Dataset[_] = {
      def condition(col: Column): Column = colCondition

      df.filter(cols.map(c ⇒ condition(c)).reduce(_ and _))
    }

    /**
     * Removing Leading Zeros in mobile number
     */
    def removeLeadingZeros(columnName: String): Dataset[_] = {

      df.withColumn(columnName, regexp_replace(col(columnName), "^0+", ""))

    }

    /**
     * Select latest of 2 records with similar concatId
     */
    def selectLatestRecord(implicit spark: SparkSession): Dataset[_] = {
      import spark.implicits._
      val w2 = Window.partitionBy($"concatId")
      df
        .withColumn("count", count("*").over(w2))
        .withColumn("select", when($"count" > 1, $"inDelta").otherwise(lit(true))) // select latest record
        .filter($"select")
        .drop("select", "count")
    }

    def grabOneRecordPerGroup(implicit spark: SparkSession): Dataset[_] = {
      import spark.implicits._
      val w = Window.partitionBy("ohubId").orderBy($"concatId")
      df.withColumn("rn", row_number().over(w))
        .filter($"rn" === 1)
        .drop("rn")
    }
  }

}
