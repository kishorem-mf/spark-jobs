package com.unilever.ohub.spark.insights

import com.unilever.ohub.spark.SparkJobSpec

class BaseInsightUtilsSpec extends SparkJobSpec {

  describe("getSourceAndModelName") {
    it("should return valid source and model name for ContactPerson") {
      val fileName = "UFS_OHUB1_CONTACTPERSONS_20200220101010"
      val (sourceName, modelName) = BaseInsightUtils.getModelAndSourceName(fileName)

      sourceName shouldBe "OHUB1"
      modelName shouldBe "CONTACTPERSONS"
    }
  }

    describe("getSourceAndModelName") {
      it("should return valid source and model name for Orders Deleted") {
        val fileName = "UFS_FUZZIT_ORDERS_DELETED_20200220101010"
        val (sourceName, modelName) = BaseInsightUtils.getModelAndSourceName(fileName)

        sourceName shouldBe "FUZZIT"
        modelName shouldBe "ORDERS_DELETED"
      }
    }

    describe("getSourceAndModelName") {
      it("should return valid source and model name for ONE_MOBILE and ContactPerson_Activities") {
        val fileName = "UFS_ONE_MOBILE_CONTACTPERSON_ACTIVITIES_20200220101010"
        val (sourceName, modelName) = BaseInsightUtils.getModelAndSourceName(fileName)

        sourceName shouldBe "ONE_MOBILE"
        modelName shouldBe "CONTACTPERSON_ACTIVITIES"
      }
    }

    describe("getSourceAndModelName") {
      it("should return empty for invalid filename") {
        val fileName = "UFS_CONTACTPERSONS_20200220101010"
        val (sourceName, modelName) = BaseInsightUtils.getModelAndSourceName(fileName)

        sourceName shouldBe "-"
        modelName shouldBe "-"
      }
    }
}
