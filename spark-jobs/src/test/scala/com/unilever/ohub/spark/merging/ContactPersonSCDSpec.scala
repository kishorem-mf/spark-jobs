package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.domain.entity.{ContactPerson, Operator, TestOperators}
import org.apache.spark.sql.Encoders

class ContactPersonSCDSpec extends SparkJobSpec with TestOperators {

  implicit val encoder = Encoders.product[ContactPerson]

  describe("Change log SCD for operator entity") {
    it("should generate SCD change log contact persons ") {

      val changeLogPrevious = spark.read.parquet(getClass.getResource("/change_log/")
        .getPath + "2019-08-26/contactpersons_change_log.parquet")

      val contactPersons=spark
        .read
        .schema(encoder.schema)
        .parquet(getClass.getResource("/change_log/").getPath + "2019-08-27/contactpersons.parquet").as[ContactPerson]

      import spark.implicits._
      val change_log_cp=ContactPersonSCD.transform(spark,contactPersons,changeLogPrevious)
        .filter($"concatid"==="AE~ARMSTRONG~114100").orderBy("fromDate")
      
      change_log_cp.select("ohubId").first().getString(0) shouldBe "417d51e8-96be-446f-b921-393f6f34e74a"
      change_log_cp.select("toDate").take(4).last.anyNull shouldBe true
      change_log_cp.select("fromDate").take(4).last.getDate(0).toString shouldBe "2019-08-27"

    }
  }
}
