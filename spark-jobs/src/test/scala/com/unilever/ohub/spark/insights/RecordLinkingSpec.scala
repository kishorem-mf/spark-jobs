package com.unilever.ohub.spark.insights

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity._
import org.apache.spark.sql.Dataset
import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.storage.{DefaultStorage}


class RecordLinkingSpec extends SparkJobSpec {

  import spark.implicits._

  private val SUT = BaseInsightUtils


  describe("Record Linking") {

    it("should give linking and not linking count") {
      val storage = new DefaultStorage(spark)
      
    }

  }
}
