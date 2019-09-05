package com.unilever.ohub.spark.export.acm

import org.scalatest.FunSpec

class OldOhubConverterTest extends FunSpec {

  describe("When source is unknown") {
    it("Should leave out the source id") {
      val sourceIds = Map("FILE" -> 1)
      assert(new OperatorOldOhubConverter(sourceIds).impl("a~b~c") == "a~c~1~")
    }
  }

  describe("When source is known") {
    val sourceIds = Map("FILE" -> 122)
    it("Should add sourceId") {
      assert(new OperatorOldOhubConverter(sourceIds).impl("a~FILE~c") == "a~c~1~122")
    }
    it("Should still work when source id contains a ~") {
      assert(new OperatorOldOhubConverter(sourceIds).impl("ES~FILE~O~247087") == "ES~O~247087~1~122")
    }
  }
}
