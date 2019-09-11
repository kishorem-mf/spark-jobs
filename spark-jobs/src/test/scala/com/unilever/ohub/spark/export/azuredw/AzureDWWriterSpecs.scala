package com.unilever.ohub.spark.export.azuredw

import java.sql.Timestamp

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.TestContactPersons
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, functions}

class AzureDWWriterSpecs extends SparkJobSpec with TestContactPersons {

  import spark.implicits._

  private val SUT = ContactPersonDWWriter

  describe("Azure DataWarehouse Writer") {

    val cp1 = defaultContactPerson.copy(firstName = Some("a" * 4050), ohubId = Some("G"),
      dateUpdated = None,
      dateCreated = Some(Timestamp.valueOf("2015-09-30 14:23:05.0"))
    )
    val cp2 = defaultContactPerson.copy(firstName = Some("b"), ohubId = Some("S"),
      dateUpdated = None,
      dateCreated = Some(Timestamp.valueOf("2015-09-30 14:23:05.0"))
    )
    val cp3 = defaultContactPerson.copy(firstName = Some("c"), ohubId = Some("V"),
      dateUpdated = None,
      dateCreated = Some(Timestamp.valueOf("2015-09-30 14:23:05.0"))
    )

    val contactPersonsDF = Seq(cp1, cp2, cp3).toDataset.toDF

    val result: DataFrame = SUT.transform(contactPersonsDF)

    it("No rows are filtered out") {
      assert(result.count == 3)
    }

    it("Doesn't contain Map/Array Fields") {
      val mapFields: Array[String] = result.schema.fields.collect(
        { case field if field.dataType.typeName == "map" || field.dataType.typeName == "array" ⇒ field.name })
      assert(mapFields.length == 0)
    }

    it("String Fields are truncated to max 4000 characters because of Azure DW restrictions") {

      val stringFields: Array[String] = result.schema.fields.collect(
        { case field if field.dataType.typeName == "string" ⇒ field.name })

      stringFields.foreach(field => {
        val df: DataFrame = result.select(col(field))
        val truncColumn = df.agg(functions.max(functions.length(col(field)))).as[Option[Int]].first.fold(true)(x => x <= 4000)
        assert(truncColumn)
      })
    }

    it("Date Updated is replaced with datecreated when null") {

      val tcResult = result.filter($"ohubId" === "G")
      tcResult.select($"dateUpdated").first shouldBe tcResult.select($"dateCreated").first
    }

  }
}
