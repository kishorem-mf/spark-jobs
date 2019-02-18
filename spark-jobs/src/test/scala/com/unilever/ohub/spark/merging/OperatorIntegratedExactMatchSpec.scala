package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.{ Operator, TestOperators }
import org.apache.spark.sql.{ Dataset, SparkSession }

case class ResultOperator(
    sourceEntityId: String,
    ohubId: Option[String],
    name: Option[String],
    street: Option[String] = None)

class OperatorIntegratedExactMatchSpec extends SparkJobSpec with TestOperators {

  import spark.implicits._

  lazy implicit val implicitSpark: SparkSession = spark

  private val recordA1 = defaultOperatorWithSourceEntityId("100").copy(ohubId = Some("AAA"), name = Some("Jan"))
  private val recordA1_2 = defaultOperatorWithSourceEntityId("101").copy(ohubId = Some("AAA"), name = Some("Jan"))
  private val recordB1 = defaultOperatorWithSourceEntityId("200").copy(ohubId = Some("BBB"), name = Some("Piet"))
  private val recordB1_2 = defaultOperatorWithSourceEntityId("201").copy(ohubId = Some("BBB"), name = Some("Piet"))
  private val recordA2 = defaultOperatorWithSourceEntityId("200").copy(ohubId = None, name = Some("Jan"))

  private val recordC1 = defaultOperatorWithSourceEntityId("300").copy(ohubId = None, name = Some("Jan"), street = Some("street1"))
  private val recordC1_2 = defaultOperatorWithSourceEntityId("301").copy(ohubId = None, name = Some("Jan"), street = Some("street1"))
  private val recordD1 = defaultOperatorWithSourceEntityId("400").copy(ohubId = None, name = Some("Jan"), street = Some("street2"))
  private val recordD1_2 = defaultOperatorWithSourceEntityId("401").copy(ohubId = None, name = Some("Jan"), street = Some("street2"))

  private val COPY_GENERATED = "COPY_GENERATED"

  describe("OperatorIntegratedExactMatch.transform") {
    it("should add a new entity to a new group in integrated") {
      val integratedRecords = createDataset(recordA1, recordA1_2)
      val deltaRecords = createDataset(recordB1, recordB1_2)

      val expectedMatchedExact = Set(
        ResultOperator("100", Some("AAA"), Some("Jan"), Some("street")),
        ResultOperator("101", Some("AAA"), Some("Jan"), Some("street")),
        ResultOperator("200", Some("BBB"), Some("Piet"), Some("street")),
        ResultOperator("201", Some("BBB"), Some("Piet"), Some("street"))
      )
      val expectedUnmatchedIntegrated = Set[ResultOperator]()
      val expectedUnmatchedDelta = Set[ResultOperator]()

      matchExactAndAssert(integratedRecords, deltaRecords, expectedMatchedExact, expectedUnmatchedIntegrated, expectedUnmatchedDelta)
    }

    it("should add a new entity to an existing group in integrated") {
      val integratedContactPersons = createDataset(recordA1)
      val deltaContactPersons = createDataset(recordA2)

      val expectedMatchedExact = Set(
        ResultOperator("100", Some("AAA"), Some("Jan"), Some("street")),
        ResultOperator("200", Some("AAA"), Some("Jan"), Some("street"))
      )
      val expectedUnmatchedIntegrated = Set[ResultOperator]()
      val expectedUnmatchedDelta = Set[ResultOperator]()

      matchExactAndAssert(integratedContactPersons, deltaContactPersons, expectedMatchedExact, expectedUnmatchedIntegrated, expectedUnmatchedDelta)
    }

    it("should create separate groups if not all columns are equal") {
      val integratedContactPersons = createDataset(recordC1, recordC1_2)
      val deltaContactPersons = createDataset(recordD1, recordD1_2)

      val expectedMatchedExact = Set(
        ResultOperator("300", Some(COPY_GENERATED), Some("Jan"), street = Some("street1")),
        ResultOperator("301", Some(COPY_GENERATED), Some("Jan"), street = Some("street1")),
        ResultOperator("400", Some(COPY_GENERATED), Some("Jan"), street = Some("street2")),
        ResultOperator("401", Some(COPY_GENERATED), Some("Jan"), street = Some("street2"))
      )
      val expectedUnmatchedIntegrated = Set[ResultOperator]()
      val expectedUnmatchedDelta = Set[ResultOperator]()

      matchExactAndAssert(integratedContactPersons, deltaContactPersons, expectedMatchedExact, expectedUnmatchedIntegrated, expectedUnmatchedDelta)
    }

    it("should make sure groups of size 1 are part of leftovers") {
      val integratedContactPersons = createDataset(recordA1, recordC1, recordC1_2)
      val deltaContactPersons = createDataset(recordB1, recordD1, recordD1_2)

      val expectedMatchedExact = Set(
        ResultOperator("300", Some(COPY_GENERATED), Some("Jan"), street = Some("street1")), // C1
        ResultOperator("301", Some(COPY_GENERATED), Some("Jan"), street = Some("street1")), // C1_2
        ResultOperator("400", Some(COPY_GENERATED), Some("Jan"), street = Some("street2")), // D1
        ResultOperator("401", Some(COPY_GENERATED), Some("Jan"), street = Some("street2")) // D1_2
      )
      val expectedUnmatchedIntegrated = Set(
        ResultOperator("100", Some("AAA"), Some("Jan"), Some("street")) // A1
      )
      val expectedUnmatchedDelta = Set(
        ResultOperator("200", Some("BBB"), Some("Piet"), Some("street")) // B1
      )

      matchExactAndAssert(integratedContactPersons, deltaContactPersons, expectedMatchedExact, expectedUnmatchedIntegrated, expectedUnmatchedDelta)
    }
  }

  private def matchExactAndAssert(
    integratedRecords: Dataset[Operator],
    deltaRecords: Dataset[Operator],
    expectedMatchedExact: Set[ResultOperator],
    expectedUnmatchedIntegrated: Set[ResultOperator],
    expectedUnmatchedDelta: Set[ResultOperator]
  ): Dataset[Operator] = {
    val (matchedExact, unmatchedIntegrated, unmatchedDelta) = OperatorIntegratedExactMatch.transform(
      integratedRecords, deltaRecords
    )

    val actualMatchedExact = matchedExact.collect().toSeq.map(cp ⇒ cp.sourceEntityId -> cp).toMap
    val ohubIdsCopied = expectedMatchedExact.map { result ⇒
      if (result.ohubId.contains(COPY_GENERATED)) {
        result.copy(ohubId = actualMatchedExact.get(result.sourceEntityId).flatMap(_.ohubId))
      } else {
        result
      }
    }

    assertResults(unmatchedIntegrated, expectedUnmatchedIntegrated)
    assertResults(unmatchedDelta, expectedUnmatchedDelta)
    assertResults(actualMatchedExact.values.toSeq.toDataset, ohubIdsCopied)

    matchedExact
  }

  private def assertResults(actualDataSet: Dataset[Operator], expectedResults: Set[ResultOperator]): Unit =
    createActualResultSet(actualDataSet) shouldBe expectedResults

  private def createActualResultSet(actualDataSet: Dataset[Operator]): Set[ResultOperator] =
    actualDataSet.map(cp ⇒ ResultOperator(cp.sourceEntityId, cp.ohubId, cp.name, cp.street)).collect().toSet

  private def createDataset(contactPersons: Operator*): Dataset[Operator] = contactPersons.toDataset
}

