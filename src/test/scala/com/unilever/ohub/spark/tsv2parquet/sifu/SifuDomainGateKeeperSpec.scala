package com.unilever.ohub.spark.tsv2parquet.sifu

import com.unilever.ohub.spark.SparkJobSpec

import scala.util.Success

class SifuDomainGateKeeperSpec extends SparkJobSpec {

  describe("creating URI") {
    it("should return a valid URI") {
      val url = "https://sifu.unileversolutions.com:443/products/NL/nl/0/1"
      val created = ProductConverter.createSifuURL("NL", "nl", "products", 0, 1)

      created shouldBe Success
      created.get.toString shouldBe url
    }
  }
  describe("response body") {
    it("should return ") {
      val url = ProductConverter.createSifuURL("NL", "nl", "products", 0, 1).get
      val response = ProductConverter.getResponseBodyString(url)
      println(response)
    }
  }

}
