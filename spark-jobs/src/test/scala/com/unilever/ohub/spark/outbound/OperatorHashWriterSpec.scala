package com.unilever.ohub.spark.outbound

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.{ Operator, TestOperators }
import com.unilever.ohub.spark.SharedSparkSession._
import com.unilever.ohub.spark.domain.DomainEntityHash
import org.apache.spark.sql.Dataset

class OperatorHashWriterSpec extends SparkJobSpec with TestOperators {
  import spark.implicits._

  val SUT: OperatorHashWriter.type = OperatorHashWriter

  describe("OperatorHashWriter") {
    it("should mark entity has changed when there is no previous hash") {
      val integratedEntities: Dataset[Operator] = Seq[Operator](
        defaultOperator.copy(id = "1", concatId = "1", ohubId = Some("1"))
      ).toDataset
      val previousHashes: Dataset[DomainEntityHash] = Seq[DomainEntityHash]().toDataset

      val result = SUT.determineHashes(spark, integratedEntities, previousHashes)
      result.count() shouldBe 1
      result.head().hasChanged shouldBe Some(true)
    }

    it("should mark entity has changed when hashes don't match") {
      val integratedEntities: Dataset[Operator] = Seq[Operator](
        defaultOperator.copy(id = "1", concatId = "1", ohubId = Some("1"))
      ).toDataset
      val previousHashes: Dataset[DomainEntityHash] = Seq[DomainEntityHash](
        DomainEntityHash("1", Some(false), Some("previous-hash"))
      ).toDataset

      val result = SUT.determineHashes(spark, integratedEntities, previousHashes)
      result.count() shouldBe 1
      result.head().hasChanged shouldBe Some(true)
    }

    it("should mark entity has changed when operator has changed") {
      val integratedEntities: Dataset[Operator] = Seq[Operator](
        defaultOperator.copy(id = "1", concatId = "1", ohubId = Some("1"), name = Some("a new name"))
      ).toDataset
      val previousHashes: Dataset[DomainEntityHash] = Seq[DomainEntityHash](
        DomainEntityHash("1", Some(false), Some("3ca8c46280aad881648b7df74bc1363"))
      ).toDataset

      val result = SUT.determineHashes(spark, integratedEntities, previousHashes)
      result.count() shouldBe 1
      result.head().hasChanged shouldBe Some(true)
    }

    it("should mark entity has not changed when the hashes match") {
      val integratedEntities: Dataset[Operator] = Seq[Operator](
        defaultOperator.copy(id = "1", concatId = "1", ohubId = Some("1"))
      ).toDataset
      val previousHashes: Dataset[DomainEntityHash] = Seq[DomainEntityHash](
        DomainEntityHash("1", Some(true), Some("3ca8c46280aad881648b7df74bc1363"))
      ).toDataset

      val result = SUT.determineHashes(spark, integratedEntities, previousHashes)
      result.count() shouldBe 1
      result.head().hasChanged shouldBe Some(false)
    }
  }
}
