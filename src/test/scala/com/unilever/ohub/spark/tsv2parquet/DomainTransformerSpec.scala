package com.unilever.ohub.spark.tsv2parquet

import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ DataTypes, StructField, StructType }
import org.scalamock.scalatest.MockFactory
import org.scalatest.{ Matchers, WordSpec }
import com.unilever.ohub.spark.tsv2parquet.CustomParsers._

class DomainTransformerSpec extends WordSpec with Matchers with MockFactory {

  val domainTransformer = DomainTransformer(TestDomainDataProvider())
  val domainFieldName = "domain-field-name"
  val originalColumnName = "original"

  "Domain transformer" should {
    "throw an illegal argument exception" when {
      "an original column name does not exist" in {
        val row = mock[Row]

        (row.fieldIndex(_: String)).expects(originalColumnName).throwing(new IllegalArgumentException(s"Fieldname '$originalColumnName' does not exist."))
        (row.fieldIndex(_: String)).expects(s"${DomainTransformer.ZERO_WIDTH_NO_BREAK_SPACE}$originalColumnName").throwing(new IllegalArgumentException(s"Fieldname '$originalColumnName' does not exist."))

        intercept[IllegalArgumentException] {
          domainTransformer.mandatory(originalColumnName, domainFieldName)(row)
        }
      }
    }

    "throw a mandatory constraint exception" when {
      "an original column has a null value" in {
        val row = new GenericRowWithSchema(List(null).toArray, StructType(List(StructField(originalColumnName, DataTypes.StringType, nullable = true))))

        val actualException = intercept[MandatoryFieldException] {
          domainTransformer.mandatory(originalColumnName, domainFieldName)(row)
        }

        assertMandatoryFieldException(domainFieldName, s"No value found for '$originalColumnName'", actualException)
      }

      "an original column has an invalid Int value" in {
        val row = new GenericRowWithSchema(List("abc").toArray, StructType(List(StructField(originalColumnName, DataTypes.LongType, nullable = true))))

        val actualException = intercept[MandatoryFieldException] {
          domainTransformer.mandatory[Long](originalColumnName, domainFieldName, toLong)(row)
        }

        assertMandatoryFieldException(domainFieldName, s"Couldn't apply transformation function on value 'Some(abc)'", actualException)
      }

      "use mandatoryValue to retrieve a value and there is no value for an original column" in {
        val row = new GenericRowWithSchema(List(null).toArray, StructType(List(StructField(originalColumnName, DataTypes.StringType, nullable = true))))

        intercept[MandatoryFieldException] {
          domainTransformer.mandatoryValue(originalColumnName, domainFieldName)(row)
        }
      }
    }

    "resolve a mandatory value" when {
      "an original column has a valid value" in {
        val row = new GenericRowWithSchema(List("123456").toArray, StructType(List(StructField(originalColumnName, DataTypes.LongType, nullable = true))))

        val value: Long = domainTransformer.mandatory[Long](originalColumnName, domainFieldName, toLong)(row)

        value shouldBe 123456
      }

      "se mandatoryValue to retrieve a value and an original column has a valid value" in {
        val row = new GenericRowWithSchema(List("123456").toArray, StructType(List(StructField(originalColumnName, DataTypes.StringType, nullable = true))))

        val value: String = domainTransformer.mandatoryValue(originalColumnName, domainFieldName)(row)

        value shouldBe "123456"
      }
    }

    "throw an illegal argument exception" when {
      "and optional column does not exist" in {
        val row = mock[Row]

        (row.fieldIndex(_: String)).expects(originalColumnName).throwing(new IllegalArgumentException(s"Fieldname '$originalColumnName' does not exist."))
        (row.fieldIndex(_: String)).expects(s"${DomainTransformer.ZERO_WIDTH_NO_BREAK_SPACE}$originalColumnName").throwing(new IllegalArgumentException(s"Fieldname '$originalColumnName' does not exist."))

        intercept[IllegalArgumentException] {
          domainTransformer.optional(originalColumnName, domainFieldName)(row)
        }
      }
    }

    "resolve to None" when {
      "an original column has a null value" in {
        val row = new GenericRowWithSchema(List(null).toArray, StructType(List(StructField(originalColumnName, DataTypes.StringType, nullable = true))))

        val value = domainTransformer.optional(originalColumnName, domainFieldName)(row)

        value shouldBe None
        domainTransformer.errors shouldBe Map()
      }

      "an original column has an empty value" in {
        val row = new GenericRowWithSchema(List("").toArray, StructType(List(StructField(originalColumnName, DataTypes.StringType, nullable = true))))

        val value = domainTransformer.optional(originalColumnName, domainFieldName)(row)

        value shouldBe None
        domainTransformer.errors shouldBe Map()
      }
    }

    "register an error" when {
      "an original column has an invalid Int value" in {
        val row = new GenericRowWithSchema(List("abc").toArray, StructType(List(StructField(originalColumnName, DataTypes.IntegerType, nullable = true))))

        val value = domainTransformer.optional[Int](originalColumnName, domainFieldName, toInt)(row)

        value shouldBe None
        domainTransformer.errors shouldBe Map("domain-field-name" -> IngestionError(originalColumnName, Some("abc"), "java.lang.NumberFormatException:For input string: \"abc\""))
      }
    }

    "resolve an optional value" when {
      "an original column has a valid value" in {
        val row = new GenericRowWithSchema(List("123456").toArray, StructType(List(StructField(originalColumnName, DataTypes.IntegerType, nullable = true))))

        val value: Option[Int] = domainTransformer.optional[Int](originalColumnName, domainFieldName, toInt)(row)

        value shouldBe Some(123456)
      }
    }

    "use headers" when {
      "no header is provided" in {
        val row = new GenericRowWithSchema(List("123456").toArray, StructType(List(StructField(originalColumnName, DataTypes.IntegerType, nullable = true))))

        domainTransformer.useHeaders(Map(originalColumnName -> 0))

        val value = domainTransformer.optionalValue(originalColumnName)(row)
        value shouldBe Some("123456")
      }
    }

    "add an additional field" when {
      "a additional value is provided correctly" in {
        val row = new GenericRowWithSchema(List("some-value").toArray, StructType(List(StructField(originalColumnName, DataTypes.StringType, nullable = true))))

        val value = domainTransformer.additionalField[String](originalColumnName, "additional-field-name")(row)

        value shouldBe Some("some-value")
        domainTransformer.additionalFields shouldBe Map("additional-field-name" -> "some-value")
      }
    }
  }

  private def assertMandatoryFieldException(domainFieldName: String, errorMessage: String, actualException: MandatoryFieldException) = {
    val expectedException = MandatoryFieldException(domainFieldName, errorMessage)
    actualException.getMessage shouldBe expectedException.getMessage
  }
}
