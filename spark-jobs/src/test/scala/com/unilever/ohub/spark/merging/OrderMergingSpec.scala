package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity._
import org.apache.spark.sql.Dataset
import com.unilever.ohub.spark.SharedSparkSession.spark

class OrderMergingSpec extends SparkJobSpec with TestOrders with TestOperators with TestContactPersons {

  import spark.implicits._

  private val SUT = OrderMerging

  describe("order merging") {
    it("a new order should get an ohubId and orderUid and be marked golden record") {
      val newRecord = defaultOrder.copy(
        isGoldenRecord = false,
        concatId = "new"
      )

      val input = Seq[Order](
        newRecord
      ).toDataset
      val previous = Seq[Order]().toDataset
      val operators = Seq[Operator]().toDataset
      val contactPersons = Seq[ContactPerson]().toDataset

      val result = SUT.transform(spark, input, previous, operators, contactPersons)
        .collect()

      result.size shouldBe 1
      result.head.isGoldenRecord shouldBe true
      result.head.ohubId shouldBe defined
      result.head.orderUid shouldBe defined
    }

    it("should favor delta orderUid over integrated") {
      val operators = Seq[Operator]().toDataset
      val contactPersons = Seq[ContactPerson]().toDataset

      val integratedRecord = defaultOrder.copy(
        orderUid = Some("a")
      )

      val newRecord = defaultOrder.copy(
        orderUid = Some("b")
      )

      val previous: Dataset[Order] = spark.createDataset(Seq(
        integratedRecord
      ))

      val input: Dataset[Order] = spark.createDataset(Seq(
        newRecord
      ))

      val result = SUT.transform(spark, input, previous, operators, contactPersons)
        .collect()

      result.length shouldBe 1
      result(0).orderUid shouldBe Some("b")

    }

    it("should take newest data if available while retaining ohubId and orderUid") {
      val operators = Seq(
        defaultOperatorWithSourceName("op1").copy(ohubId = Some("ohubOp1")),
        defaultOperatorWithSourceName("op2").copy(ohubId = Some("ohubOp2")),
        defaultOperatorWithSourceName("op3").copy(ohubId = Some("ohubOp3"))
      ).toDataset

      val contactPersons = Seq(
        defaultContactPersonWithSourceEntityId("cpn1").copy(ohubId = Some("ohubCpn1")),
        defaultContactPersonWithSourceEntityId("cpn2").copy(ohubId = Some("ohubCpn2")),
        defaultContactPersonWithSourceEntityId("cpn3").copy(ohubId = Some("ohubCpn3"))
      ).toDataset

      val updatedRecord = defaultOrder.copy(
        isGoldenRecord = true,
        ohubId = Some("oldId"),
        orderUid = Some("oldOrderUid"),
        countryCode = "updated",
        concatId = s"updated~${defaultOrder.sourceName}~${defaultOrder.sourceEntityId}",
        operatorConcatId = Some("country-code~op1~source-entity-id"),
        contactPersonConcatId = Some("AU~WUFOO~cpn1"),
        comment = Some("Calve"))

      val deletedRecord = defaultOrder.copy(
        isGoldenRecord = true,
        countryCode = "deleted",
        concatId = s"deleted~${defaultOrder.sourceName}~${defaultOrder.sourceEntityId}",
        operatorConcatId = Some("country-code~op2~source-entity-id"),
        contactPersonConcatId = Some("AU~WUFOO~cpn2"),
        isActive = true)

      val newRecord = defaultOrder.copy(
        isGoldenRecord = true,
        countryCode = "new",
        concatId = s"new~${defaultOrder.sourceName}~${defaultOrder.sourceEntityId}",
        operatorConcatId = Some("country-code~op3~source-entity-id"),
        contactPersonConcatId = Some("AU~WUFOO~cpn3")
      )

      val unchangedRecord = defaultOrder.copy(
        isGoldenRecord = true,
        countryCode = "unchanged",
        concatId = s"unchanged~${defaultOrder.sourceName}~${defaultOrder.sourceEntityId}",
        operatorConcatId = Some("country-code~op4~source-entity-id"),
        contactPersonConcatId = Some("AU~WUFOO~cpn4")
      )

      val notADeltaRecord = defaultOrder.copy(
        isGoldenRecord = true,
        countryCode = "notADelta",
        concatId = s"notADelta~${defaultOrder.sourceName}~${defaultOrder.sourceEntityId}",
        operatorConcatId = Some("country-code~op5~source-entity-id"),
        contactPersonConcatId = Some("AU~WUFOO~cpn5")
      )

      val previous: Dataset[Order] = spark.createDataset(Seq(
        updatedRecord,
        deletedRecord,
        unchangedRecord,
        notADeltaRecord
      ))
      val input: Dataset[Order] = spark.createDataset(Seq(
        updatedRecord.copy(comment = Some("Unox"), ohubId = Some("newId")),
        deletedRecord.copy(isActive = false),
        unchangedRecord,
        newRecord
      ))

      val result = SUT.transform(spark, input, previous, operators, contactPersons)
        .collect()
        .sortBy(_.countryCode)

      result.length shouldBe 5
      result(0).isActive shouldBe false
      result(0).operatorOhubId shouldBe Some("ohubOp2")
      result(0).contactPersonOhubId shouldBe Some("ohubCpn2")

      result(1).countryCode shouldBe "new"
      result(1).operatorOhubId shouldBe Some("ohubOp3")
      result(1).contactPersonOhubId shouldBe Some("ohubCpn3")

      result(2).countryCode shouldBe "notADelta"
      result(2).operatorOhubId shouldBe None
      result(2).contactPersonOhubId shouldBe None

      result(3).countryCode shouldBe "unchanged"
      result(3).operatorOhubId shouldBe None
      result(3).contactPersonOhubId shouldBe None

      result(4).countryCode shouldBe "updated"
      result(4).operatorOhubId shouldBe Some("ohubOp1")
      result(4).contactPersonOhubId shouldBe Some("ohubCpn1")
      result(4).comment shouldBe Some("Unox")
      result(4).ohubId shouldBe Some("oldId")
      result(4).orderUid shouldBe Some("oldOrderUid")
    }
  }
}
