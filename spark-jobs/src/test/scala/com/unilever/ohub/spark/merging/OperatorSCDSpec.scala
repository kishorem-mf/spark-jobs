package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.domain.entity.{Operator, TestOperators}
import org.apache.spark.sql.Encoders

class OperatorSCDSpec extends SparkJobSpec with TestOperators {

  implicit val encoder = Encoders.product[Operator]

  describe("Change log SCD for operator entity") {
    it("should generate SCD change log operator ") {

       val changeLogPrevious = spark.read.parquet(getClass.getResource("/change_log/")
         .getPath + "2019-08-26/operators_change_log.parquet")

      val operators=spark
        .read
        .schema(encoder.schema)
        .parquet(getClass.getResource("/change_log/").getPath + "2019-08-27/operators.parquet").as[Operator]

      val change_log_operators=OperatorSCD.transform(spark,operators,changeLogPrevious).orderBy("fromDate")
      change_log_operators.select("ohubId").first().getString(0) shouldBe "18e7a70c-f6a4-4182-9891-4c8cdfde67b7"
      change_log_operators.select("toDate").take(4).last.anyNull shouldBe true
      change_log_operators.select("fromDate").take(4).last.getDate(0).toString shouldBe "2019-08-27"

    }
  }
}
