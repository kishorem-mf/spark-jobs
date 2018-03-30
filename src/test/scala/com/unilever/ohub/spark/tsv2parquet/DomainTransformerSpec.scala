package com.unilever.ohub.spark.tsv2parquet

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ DataTypes, StructField, StructType }
import org.scalamock.scalatest.MockFactory
import org.scalatest.{ Matchers, WordSpec }
import com.unilever.ohub.spark.tsv2parquet.CustomParsers._

class DomainTransformerSpec extends WordSpec with Matchers with MockFactory {

  val domainTransformer = new DomainTransformer()
  val domainFieldName = "domain-field-name"
  val originalColumnName = "original"

  "Domain transformer" should {
    "throw an mandatory constraint exception" when {
      "an original column name does not exist" in {
        val row = mock[Row]

        (row.fieldIndex(_: String)).expects(originalColumnName).throwing(new IllegalArgumentException(s"Fieldname '$originalColumnName' does not exist."))

        intercept[IllegalArgumentException] {
          domainTransformer.mandatory(originalColumnName , domainFieldName)(row)
        }
      }

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
          domainTransformer.mandatory[Long](originalColumnName, domainFieldName, toInt _)(row)
        }

        assertMandatoryFieldException(domainFieldName, s"Couldn't apply transformation function on value 'Some(abc)'", actualException)
      }
    }
  }

  private def assertMandatoryFieldException(domainFieldName: String, errorMessage: String, actualException: MandatoryFieldException) = {
      val expectedException = MandatoryFieldException(domainFieldName, errorMessage)
      actualException.getMessage shouldBe expectedException.getMessage
  }
}
