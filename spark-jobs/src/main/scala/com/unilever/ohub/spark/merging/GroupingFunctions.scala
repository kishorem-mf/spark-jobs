package com.unilever.ohub.spark.merging

import java.util.UUID

import com.unilever.ohub.spark.SparkJobConfig
import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.merging.DataFrameHelpers._
import com.unilever.ohub.spark.sql.JoinType
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

  def stitchOhubId[T <: DomainEntity : Encoder](
                                                 integrated: Dataset[T],
                                                 delta: Dataset[T],
                                                 toColumn: String,
                                                 fromColumn: Column,
                                                 notNullColoumns: Seq[String])(implicit spark: SparkSession): Dataset[T] = {

    val notNullCheckColumns = notNullColoumns.map(col)

    val newWithStitch = delta
      .columnsNotNullAndNotEmpty(notNullCheckColumns)
      .alias("preprocessed")
      .join(
        integrated.select("ohubId").dropDuplicates(),
        delta("oldIntegrationId") === integrated("ohubId"),"inner"
      )
      .select("preprocessed.*")
      .stitchColumns(toColumn, fromColumn)
      .as[T]

    val deltaWithoutStitchIds = delta
      .join(newWithStitch, Seq("concatId"), JoinType.LeftAnti)
      .as[T]

    deltaWithoutStitchIds
      .unionByName(newWithStitch)
      .as[T]
  }

  def doPreProcess[Operator](data: Dataset[Operator]): Dataset[Operator] = {
    val drop_chars = "\\\\!#%&()*+-/:;<=>?@\\^|~\u00A8\u00A9\u00AA\u00AC\u00AD\u00AF\u00B0" +
      "\u00B1\u00B2\u00B3\u00B6\u00B8\u00B9\u00BA\u00BB\u00BC\u00BD\u00BE" +
      "\u2013\u2014\u2022\u2026\u20AC\u2121\u2122\u2196\u2197\u247F\u250A" +
      "\u2543\u2605\u2606\u3001\u3002\u300C\u300D\u300E\u300F\u3010\u3011" +
      "\uFE36\uFF01\uFF06\uFF08\uFF09\uFF1A\uFF1B\uFF1F{}\u00AE\u00F7\u02F1" +
      "\u02F3\u02F5\u02F6\u02F9\u02FB\u02FC\u02FD\u1BFC\u1BFD\u2260\u2264" +
      "\u2DE2\u2DF2\uEC66\uEC7C\uEC7E\uED2B\uED34\uED3A\uEDAB\uEDFC\uEE3B" +
      "\uEEA3\uEF61\uEFA2\uEFB0\uEFB5\uEFEA\uEFED\uFDAB\uFFB7\u007F\u24D2" +
      "\u2560\u2623\u263A\u2661\u2665\u266A\u2764\uE2B1\uFF0D"
    val regex = "[{}]".format(drop_chars)
    data.withColumn("group", regexp_replace(data("group"), regex, ""))
    data
  }

  def matchColumns[T <: DomainEntity : Encoder](
                                                 integrated: Dataset[T],
                                                 delta: Dataset[T],
                                                 groupingColumns: Seq[String],
                                                 notNullColoumns: Seq[String],
                                                 preProcess: Boolean = false)(implicit spark: SparkSession): Dataset[T] = {

    val sameColumns = groupingColumns.map(col)
    val notNullCheckColumns = notNullColoumns.map(col)

    val integratedWithExact = integrated
      .columnsNotNullAndNotEmpty(notNullCheckColumns)
      .concatenateColumns("group", sameColumns,preProcess)
      .withColumn("inDelta", lit(false))

    val newWithExact = delta
      .columnsNotNullAndNotEmpty(notNullCheckColumns)
      .concatenateColumns("group", sameColumns,preProcess)
      .withColumn("inDelta", lit(true))

    if(preProcess) {
      integratedWithExact
        .union(newWithExact)
        .addOhubId
        .drop("group")
        .selectLatestRecord
        .drop("inDelta")
        .as[T]
    } else {
      val preProcessedData=doPreProcess(integratedWithExact
        .unionByName(newWithExact))

      preProcessedData
        .addOhubId
        .drop("group")
        .selectLatestRecord
        .drop("inDelta")
        .as[T]
      }


  }
}

object DataFrameHelpers extends GroupingFunctions {

  implicit class Helpers(df: Dataset[_]) {

    /**
      * pre process the columns provided
      *
      * @param c                 create new columns
      */
    private def applyPreProcess(c: Column): Column = {
      val pattern="[ \u0024\u00A2\u00A3\u00A4\u00A5\u058F\u060B\u09F2\u09F3\u09FB\u0AF1\u0BF9\u0E3F\u17DB\u20A0\u20A1\u20A2" +
      "\u20A3\u20A4\u20A5\u20A6\u20A7\u20A8\u20A9\u20AA\u20AB\u20AC\u20AD\u20AE\u20AF\u20B0\u20B1\u20B2\u20B3\u20B4" +
      "\u20B5\u20B6\u20B7\u20B8\u20B9\u20BA\u20BB\u20BC\u20BD\u20BE\uA838\uFDFC\uFE69\uFF04\uFFE0\uFFE1\uFFE5\uFFE6" +
      "\u0081°”\\\\_\\'\\~`!@#$%()={}|:;\\?/<>,\\.\\[\\]\\+\\-\\*\\^&:]+"
      regexp_replace(trim(lower(c)),pattern,"")
    }


    /**
     * Concatenate all the specified
     *
     * @param cols               and create new column with cols
     * @param name               and create new column with name
     * @param pre_process is used to allow pre process. By default it is false
     */
    def concatenateColumns(name: String, cols: Seq[Column], pre_process: Boolean = false)(implicit spark: SparkSession): Dataset[_] = {
      df.withColumn(name,
        concat(cols.map(c ⇒
          when(c.isNull, "").otherwise(
            if (!pre_process) {
              c
            } else {
              applyPreProcess(c)
            }
          )
        ): _*))
    }


    /**
      * Stitch all the specified
      *
      * @param colTo               and create to column with name
      * @param colFrom             and create from column with name
      */
    def stitchColumns( colTo: String, colFrom: Column)(implicit spark: SparkSession): Dataset[_] = {
      df.withColumn(colTo,colFrom)
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
    def columnsCondition(cols: Seq[Column], colCondition: Column): Dataset[_] = {
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
