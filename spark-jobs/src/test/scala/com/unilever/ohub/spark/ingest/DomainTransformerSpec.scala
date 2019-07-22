package com.unilever.ohub.spark.ingest

import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import com.unilever.ohub.spark.ingest.CustomParsers._
import com.unilever.ohub.spark.ingest.DomainTransformer.ZERO_WIDTH_NO_BREAK_SPACE
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{Matchers, WordSpec}

class DomainTransformerSpec extends WordSpec with Matchers with MockFactory {

  val domainTransformer = DomainTransformer()
  val domainFieldName = "domain-field-name"

  "Domain transformer" should {
    "throw an illegal argument exception" when {
      "an domain-field-name column name does not exist" in {
        val row = mock[Row]

        expectColumnNameNotFound(row)

        intercept[IllegalArgumentException] {
          domainTransformer.mandatory(domainFieldName)(row)
        }
      }
    }

    "throw a mandatory constraint exception" when {
      "an domainFieldName column has a null value" in {
        val row = new GenericRowWithSchema(List(null).toArray, StructType(List(StructField(domainFieldName, DataTypes.StringType, nullable = true))))

        val actualException = intercept[MandatoryFieldException] {
          domainTransformer.mandatory(domainFieldName)(row)
        }

        assertMandatoryFieldException(domainFieldName, s"No value found for '$domainFieldName'", actualException)
      }

      "an domainFieldName column has an invalid Int value" in {
        val row = new GenericRowWithSchema(List("abc").toArray, StructType(List(StructField(domainFieldName, DataTypes.LongType, nullable = true))))

        val actualException = intercept[MandatoryFieldException] {
          domainTransformer.mandatory[Long](domainFieldName, toLong)(row)
        }

        assertMandatoryFieldException(domainFieldName, s"Couldn't apply transformation function on value 'Some(abc)'", actualException)
      }
    }

    "resolve a mandatory value" when {
      "an domain-field-name column has a valid value" in {
        val row = new GenericRowWithSchema(List("123456").toArray, StructType(List(StructField(domainFieldName, DataTypes.LongType, nullable = true))))

        val value: Long = domainTransformer.mandatory[Long](domainFieldName, toLong)(row)

        value shouldBe 123456
      }
    }

    "throw an illegal argument exception" when {
      "a mandatory column does not exist" in {
        val row = mock[Row]

        expectColumnNameNotFound(row)

        intercept[IllegalArgumentException] {
          domainTransformer.mandatory(domainFieldName)(row)
        }
      }
    }

    "resolve to None" when {
      "an domain-field-name column has a null value" in {
        val row = new GenericRowWithSchema(List(null).toArray, StructType(List(StructField(domainFieldName, DataTypes.StringType, nullable = true))))

        val value = domainTransformer.optional(domainFieldName)(row)

        value shouldBe None
        domainTransformer.errors shouldBe Map()
      }

      "an domain-field-name column has an empty value" in {
        val row = new GenericRowWithSchema(List("").toArray, StructType(List(StructField(domainFieldName, DataTypes.StringType, nullable = true))))

        val value = domainTransformer.optional(domainFieldName)(row)

        value shouldBe None
        domainTransformer.errors shouldBe Map()
      }

      "an optional column does not exist" in {
        val row = mock[Row]
        expectColumnNameNotFound(row)

        val value = domainTransformer.optional(domainFieldName)(row)

        value shouldBe None
        domainTransformer.errors shouldBe Map()
      }
    }

    "register an error" when {
      "an domain-field-name column has an invalid Int value" in {
        val row = new GenericRowWithSchema(List("abc").toArray, StructType(List(StructField(domainFieldName, DataTypes.IntegerType, nullable = true))))

        val value = domainTransformer.optional[Int](domainFieldName, toInt)(row)

        value shouldBe None
        domainTransformer.errors shouldBe Map("domain-field-name" -> IngestionError(domainFieldName, Some("abc"), "java.lang.NumberFormatException:For input string: \"abc\""))
      }
    }

    "resolve an optional value" when {
      "an domain-field-name column has a valid value" in {
        val row = new GenericRowWithSchema(List("123456").toArray, StructType(List(StructField(domainFieldName, DataTypes.IntegerType, nullable = true))))

        val value: Option[Int] = domainTransformer.optional[Int](domainFieldName, toInt)(row)

        value shouldBe Some(123456)
      }
    }

    "use headers" when {
      "no header is provided" in {
        val row = new GenericRowWithSchema(List("123456").toArray, StructType(List(StructField(domainFieldName, DataTypes.IntegerType, nullable = true))))

        domainTransformer.useHeaders(Map(domainFieldName -> 0))

        val value = domainTransformer.optionalValue(domainFieldName)(row)
        value shouldBe Some("123456")
      }
    }
  }

  private def expectColumnNameNotFound(row: Row) = {
    for (i ← 0 until 100) {
      val bomChars = (0 until i + 1).map(_ ⇒ ZERO_WIDTH_NO_BREAK_SPACE).mkString("")
      val newColumnName = if (i == 0) domainFieldName else bomChars + "\"" + domainFieldName + "\""

      (row.fieldIndex(_: String))
        .expects(newColumnName)
        .throwing(new IllegalArgumentException(s"Fieldname '$newColumnName' does not exist."))
    }
  }

  private def assertMandatoryFieldException(domainFieldName: String, errorMessage: String, actualException: MandatoryFieldException) = {
    val expectedException = MandatoryFieldException(domainFieldName, errorMessage)
    actualException.getMessage shouldBe expectedException.getMessage
  }
}
