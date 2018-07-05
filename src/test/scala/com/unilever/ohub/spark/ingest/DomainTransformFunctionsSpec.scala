package com.unilever.ohub.spark.ingest

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ DataTypes, StructField, StructType }
import org.scalatest.{ Matchers, WordSpec }

class DomainTransformFunctionsSpec extends WordSpec with Matchers {

  val domainTransformer = DomainTransformer(TestDomainDataProvider())
  val domainFieldName = "domain-field-name"
  val originalColumnName = "original"

  "Domain transform functions" should {
    "split an address into street, houseNumber & extension" when {
      "input is provided correctly in" in {
        assertAddress("street 123 a", Some("street"), Some("123"), Some("a"))
        assertAddress("street 123", Some("street"), Some("123"), None)
        assertAddress("street", Some("street"), None, None)
        assertAddress("", None, None, None)
      }

    }
    "cause an ingestion error" when {
      "input is provided incorrectly" in {
        assertAddress("a street name that's way to long 12 abc", None, None, None, expectIngestionError = true)
      }
    }
  }

  private def assertAddress(originalInput: String, expectedStreet: Option[String], expectedHouseNumber: Option[String], expectedExtension: Option[String], expectIngestionError: Boolean = false): Unit = {
    val row = new GenericRowWithSchema(List(originalInput).toArray, StructType(List(StructField(originalColumnName, DataTypes.StringType, nullable = true))))

    val (street, houseNumber, extension) = domainTransformer.splitAddress(originalColumnName, "street")(row)

    street shouldBe expectedStreet
    houseNumber shouldBe expectedHouseNumber
    extension shouldBe expectedExtension

    domainTransformer.errors.get("street").isDefined shouldBe expectIngestionError
  }
}
