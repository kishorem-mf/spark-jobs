package com.unilever.ohub.spark.tsv2parquet

import org.scalatest.FunSpec
import CustomParsers._

class CustomParsersSpec extends FunSpec {

describe("checkLineLength") {
    it("should do nothing when the line length matches") {
      checkLineLength(Array("1", "2"), 2)
    }

    it("should throw an error when the line length doesn't match") {
      intercept[IllegalArgumentException] {
        checkLineLength(Array("1", "2"), 42)
      }
    }
  }

  describe("parseBoolOption") {
    it("should parse Y to true") {
      assert(parseBoolOption("Y") == Some(true))
    }
    it("should parse N to false") {
      assert(parseBoolOption("N") == Some(false))
    }
    it("should parse empty string to None") {
      assert(parseBoolOption("") == None)
    }

    
  }



}
