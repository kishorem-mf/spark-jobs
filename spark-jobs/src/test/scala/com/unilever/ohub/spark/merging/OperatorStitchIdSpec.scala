package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.{ Operator, TestOperators }
import org.apache.spark.sql.{ Dataset, SparkSession }

class OperatorStitchIdSpec extends SparkJobSpec with TestOperators {

  import spark.implicits._

  lazy implicit val implicitSpark: SparkSession = spark

  private val recordC1_2 = defaultOperatorWithSourceEntityId("301").copy(ohubId = Some("AAA"), name = Some("Jan"), street = Some("street1"), oldIntegrationId=Some("old-inte-grat-ion-id"))
  private val recordD1 = defaultOperatorWithSourceEntityId("400").copy(ohubId = None, name = Some("Jan"), street = Some("street2"), oldIntegrationId=Some("old-inte-grat-ion-id"))

  private val recordE1_2 =
    defaultOperatorWithSourceEntityId("301").copy(ohubId = Some("old-inte-grat-ion-id"), name = Some("Jan"), street = Some("street1"), oldIntegrationId=Some("old-inte-grat-ion-id"))
  private val recordE1 =
  Seq(
    defaultOperatorWithSourceEntityId("400").copy(ohubId = None, name = Some("Jan"), street = Some("street2"), oldIntegrationId=Some("old-inte-grat-ion-id")),
    defaultOperatorWithSourceEntityId("403").copy(ohubId = None, name = Some("Jan Michael"), street = Some("street3"), oldIntegrationId=Some("old-inte-grat-ion-id"))
  ).toDataset

  describe("OperatorStitchId.transform") {
    it("should not stitch if stitch ids not in Ohub id group") {
      val integratedOperators = createDataset(recordC1_2)
      val deltaOperators = createDataset(recordD1)

      val expectedStitched = Set[ResultOperator](ResultOperator("400",None,Some("Jan"),Some("street2")))

      matchExactAndAssert(integratedOperators, deltaOperators, expectedStitched)
    }

    it("should stitch if ohub ids is in Ohub id group and in ohub id format") {
      val integratedOperators = createDataset(recordE1_2)
      val deltaOperators = recordE1

      val expectedStitchedIds = Set[ResultOperator](ResultOperator("400",Some("old-inte-grat-ion-id"),Some("Jan"),Some("street2")),
                                     ResultOperator("403",Some("old-inte-grat-ion-id"),Some("Jan Michael"),Some("street3")))

      matchExactAndAssert(integratedOperators, deltaOperators, expectedStitchedIds)
    }
  }

  private def matchExactAndAssert(
                                   integratedRecords: Dataset[Operator],
                                   deltaRecords: Dataset[Operator],
                                   expectedMatchedExact: Set[ResultOperator]
                                 ): Dataset[Operator] = {
    val stitchedRecords = OperatorStitchId.transform(
      integratedRecords, deltaRecords
    )

    assertResults("stitched", stitchedRecords, expectedMatchedExact)

    stitchedRecords
  }

  private def assertResults(title: String, actualDataSet: Dataset[Operator], expectedResults: Set[ResultOperator]): Unit =
    withClue(s"$title:")(createActualResultSet(actualDataSet) shouldBe expectedResults)

  private def createActualResultSet(actualDataSet: Dataset[Operator]): Set[ResultOperator] =
    actualDataSet.map(cp ⇒ ResultOperator(cp.sourceEntityId, cp.ohubId, cp.name, cp.street)).collect().toSet

  private def createDataset(contactPersons: Operator*): Dataset[Operator] = contactPersons.toDataset
}

