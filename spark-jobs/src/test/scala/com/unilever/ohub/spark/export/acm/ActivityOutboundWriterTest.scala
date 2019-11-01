package com.unilever.ohub.spark.export.acm

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.TestActivities
import com.unilever.ohub.spark.export.OutboundConfig

class ActivityOutboundWriterTest extends SparkJobSpec {

  private val SUT = ActivityOutboundWriter

  import spark.implicits._

  val config = OutboundConfig()

  describe("Filter activities") {
    it("Should filter out activities linked to operators") {
      val result = SUT.entitySpecificFilter(spark, Seq(TestActivities.defaultActivity.copy(customerType = "OPERATOR")).toDataset, config)

      assert(result.collect().isEmpty)
    }

    it("Should not filter out activities linked to contactpersons") {
      val result = SUT.entitySpecificFilter(spark, Seq(TestActivities.defaultActivity).toDataset, config)

      assert(result.collect().size == 1)
    }
  }
}
