package com.unilever.ohub.spark.storage

import java.sql.Timestamp

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.SharedSparkSession._
import org.apache.spark.SparkException

case class SomeEntityV1(fieldA: String, fieldB: Int, fieldC: Boolean)
case class SomeEntityV2(fieldA: String, fieldD: String, fieldE: Option[Boolean])
case class SomeEntityV3(fieldA: String, fieldB: Long, fieldC: Boolean)
case class SomeEntityV4(fieldA: String, fieldD: Long)
case class SomeEntityV5(fieldA2: String, fieldB: Int, fieldC: Boolean)
case class SomeEntityV6(fieldA: String, fieldE: Timestamp)

class SchemaEvolutionSpec extends SparkJobSpec {
  import spark.implicits._

  private val storage = new DefaultStorage(spark)

  private val entitiesV1 = Seq(
    SomeEntityV1("A-1", 1, fieldC = true),
    SomeEntityV1("A-2", 2, fieldC = false)
  ).toDataset
  private val locationV1 = "spark-jobs/src/test/resources/entities_v1"

  storage.writeToParquet(entitiesV1, locationV1) // write entities in V1 data format

  describe("schema evolution") {
    it("basic adding and removing of columns is possible") {
      val entitiesV1ReadAsV1 = storage.readFromParquet[SomeEntityV1](locationV1)
      entitiesV1ReadAsV1.collect().toSet shouldBe entitiesV1.collect().toSet

      val entitiesV1ReadAsV2 = storage.readFromParquet[SomeEntityV2](locationV1)

      val entitiesV2 = Seq(
        SomeEntityV2("A-3", "3", Some(true))
      ).toDataset

      val result = entitiesV1ReadAsV2.unionByName(entitiesV2)

      result.schema shouldBe entitiesV1ReadAsV2.schema
      result.collect().toSet shouldBe Set(
        SomeEntityV2("A-1", null, None), // since in V1 there is no data for fields C & D
        SomeEntityV2("A-2", null, None),
        SomeEntityV2("A-3", "3", Some(true))
      )
    }

    it("changing the type of a column is not supported") {
      intercept[SparkException] {
        val entitiesV1ReadAsV3 = storage.readFromParquet[SomeEntityV3](locationV1)

        val entitiesV3 = Seq(
          SomeEntityV3("A-3", 3L, fieldC = true)
        ).toDataset

        entitiesV1ReadAsV3.unionByName(entitiesV3).collect()
      }
    }

    it("adding a not nullable column of type Long will lead to NPE's") {
      intercept[NullPointerException] {
        val entitiesV1ReadAsV4 = storage.readFromParquet[SomeEntityV4](locationV1)

        val entitiesV4 = Seq(
          SomeEntityV4("A-4", 4L)
        ).toDataset

        val result = entitiesV1ReadAsV4.unionByName(entitiesV4)
        result.collect().toSet
      }
    }

    it("renaming a column may lead to data loss") {
      val entitiesV1ReadAsV5 = storage.readFromParquet[SomeEntityV5](locationV1)

      val entitiesV5 = Seq(
        SomeEntityV5("A-5", 5, fieldC = false)
      ).toDataset

      val result = entitiesV1ReadAsV5.unionByName(entitiesV5)
      result.collect().toSet shouldBe Set(
        SomeEntityV5(null, 1, fieldC = true), // data for renamed column A is lost
        SomeEntityV5(null, 2, fieldC = false),
        SomeEntityV5("A-5", 5, fieldC = false)
      )
    }

    it("adding a nullable column of type Timestamp should work") {
      val entitiesV1ReadAsV6 = storage.readFromParquet[SomeEntityV6](locationV1)

      val entitiesV6 = Seq(
        SomeEntityV6("A-6", new Timestamp(1542205922011L))
      ).toDataset

      val result = entitiesV1ReadAsV6.unionByName(entitiesV6)

      result.collect().toSet shouldBe Set(
        SomeEntityV6("A-2", null),
        SomeEntityV6("A-1", null),
        SomeEntityV6("A-6", new Timestamp(1542205922011L))
      )
    }
  }
}
