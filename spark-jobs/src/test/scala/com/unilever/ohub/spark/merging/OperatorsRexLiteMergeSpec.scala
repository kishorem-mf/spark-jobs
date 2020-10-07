package com.unilever.ohub.spark.rexlite

import java.sql.Timestamp

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.TestOperatorRexlite
import org.apache.spark.sql.Row

class OperatorsRexLiteMergeSpec  extends SparkJobSpec with TestOperatorRexlite {

  private val SUT = OperatorsRexLiteMerge;

  describe("enrich operators") {
    val operators = spark.read.parquet(getClass.getResource("/rexlite/")
      .getPath + "2020-09-16/operators.parquet")

    val operators_golden=spark
      .read
      .parquet(getClass.getResource("/rexlite/").getPath + "2020-09-16/operators_golden.parquet")

    val result = SUT.transform(spark,operators.unionByName(operators_golden),operators_golden)
    it("enrich rex lite records") {

      result.select("subChannel", "concatId").collect() shouldBe Array(
        Row("A&P-Heim klass.unabh","DE~FRONTIER~0022029793-1300-21-10"),
        Row("A&P-Heim klass.unabh","DE~FRONTIER~22029793")
      )

    }
  }
}
