package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.{ ContactPerson, TestContactPersons }
import org.apache.spark.sql.Dataset

case class Result(sourceEntityId: String, isGoldenRecord: Boolean, ohubId: Option[String], firstName: Option[String], emailAddress: Option[String])

class ContactPersonIntegratedExactMatchSpec extends SparkJobSpec with TestContactPersons {
  import spark.implicits._

  private val contactPersonA1 = defaultContactPersonWithSourceEntityId("100").copy(isGoldenRecord = true, ohubId = Some("AAA"), firstName = Some("Jan"), emailAddress = Some("jan@server.com"))
  private val contactPersonB1 = defaultContactPersonWithSourceEntityId("200").copy(isGoldenRecord = true, ohubId = Some("BBB"), firstName = Some("Piet"), emailAddress = Some("piet@server.com"))
  private val contactPersonA2 = defaultContactPersonWithSourceEntityId("200").copy(isGoldenRecord = false, ohubId = None, firstName = Some("Jan"), emailAddress = Some("jan@server.com"))
  private val contactPersonA3 = defaultContactPersonWithSourceEntityId("100").copy(isGoldenRecord = false, ohubId = None, firstName = Some("Jansen"), emailAddress = Some("jan@server.com"))
  private val contactPersonA4 = defaultContactPersonWithSourceEntityId("100").copy(isGoldenRecord = false, ohubId = None, firstName = Some("Jan"), emailAddress = Some("jansen@server.com"))
  private val contactPersonB2 = defaultContactPersonWithSourceEntityId("100").copy(isGoldenRecord = false, ohubId = None, firstName = Some("Piet"), emailAddress = Some("piet@server.com"))
  private val contactPersonA5 = defaultContactPersonWithSourceEntityId("103").copy(isGoldenRecord = false, ohubId = None, firstName = Some("Jan"), emailAddress = Some("jan@server.com"))
  private val contactPersonA6 = defaultContactPersonWithSourceEntityId("200").copy(isGoldenRecord = false, ohubId = Some("AAA"), firstName = Some("Jan"), emailAddress = Some("jan@server.com"))
  private val contactPersonB3 = defaultContactPersonWithSourceEntityId("200").copy(isGoldenRecord = false, ohubId = None, firstName = Some("Kees"), emailAddress = Some("kees@server.com"))

  private val COPY_GENERATED = "COPY_GENERATED"

  describe("ContactPersonIntegratedExactMatch.transform") {
    it("should add a new entity to a new group in integrated") {
      val integratedContactPersons = createDataset(contactPersonA1)
      val deltaContactPersons = createDataset(contactPersonB1)

      val expectedMatchedExact = Set(
        Result("100", isGoldenRecord = true, Some("AAA"), Some("Jan"), Some("jan@server.com")),
        Result("200", isGoldenRecord = true, Some("BBB"), Some("Piet"), Some("piet@server.com"))
      )
      val expectedUnmatchedIntegrated = Set[Result]()
      val expectedUnmatchedDelta = Set[Result]()

      matchExactAndAssert(integratedContactPersons, deltaContactPersons, expectedMatchedExact, expectedUnmatchedIntegrated, expectedUnmatchedDelta)
    }

    it("should add a new entity to an existing group in integrated") {
      val integratedContactPersons = createDataset(contactPersonA1)
      val deltaContactPersons = createDataset(contactPersonA2)

      val expectedMatchedExact = Set(
        Result("100", isGoldenRecord = true, Some("AAA"), Some("Jan"), Some("jan@server.com")),
        Result("200", isGoldenRecord = false, Some("AAA"), Some("Jan"), Some("jan@server.com"))
      )
      val expectedUnmatchedIntegrated = Set[Result]()
      val expectedUnmatchedDelta = Set[Result]()

      matchExactAndAssert(integratedContactPersons, deltaContactPersons, expectedMatchedExact, expectedUnmatchedIntegrated, expectedUnmatchedDelta)
    }

    it("should update an existing entity in integrated when firstName changed") {
      val integratedContactPersons = createDataset(contactPersonA1)
      val deltaContactPersons = createDataset(contactPersonA3)

      val expectedMatchedExact = Set(
        Result("100", isGoldenRecord = false, Some("AAA"), Some("Jansen"), Some("jan@server.com"))
      )
      val expectedUnmatchedIntegrated = Set[Result]()
      val expectedUnmatchedDelta = Set[Result]()

      matchExactAndAssert(integratedContactPersons, deltaContactPersons, expectedMatchedExact, expectedUnmatchedIntegrated, expectedUnmatchedDelta)
    }

    it("should update an existing entity in integrated when email changed") {
      val integratedContactPersons = createDataset(contactPersonA1)
      val deltaContactPersons = createDataset(contactPersonA4)

      val expectedMatchedExact = Set(
        Result("100", isGoldenRecord = false, Some(COPY_GENERATED), Some("Jan"), Some("jansen@server.com")) // copy ohub id of actual result
      )
      val expectedUnmatchedIntegrated = Set[Result]()
      val expectedUnmatchedDelta = Set[Result]()

      matchExactAndAssert(integratedContactPersons, deltaContactPersons, expectedMatchedExact, expectedUnmatchedIntegrated, expectedUnmatchedDelta)
    }

    it("should merge two existing entities in one ohub group on exact match") {
      val integratedContactPersons = createDataset(contactPersonA1, contactPersonB1)
      val deltaContactPersons = createDataset(contactPersonB2)

      val expectedMatchedExact = Set(
        Result("100", isGoldenRecord = false, Some("BBB"), Some("Piet"), Some("piet@server.com")),
        Result("200", isGoldenRecord = true, Some("BBB"), Some("Piet"), Some("piet@server.com"))
      )
      val expectedUnmatchedIntegrated = Set[Result]()
      val expectedUnmatchedDelta = Set[Result]()

      matchExactAndAssert(integratedContactPersons, deltaContactPersons, expectedMatchedExact, expectedUnmatchedIntegrated, expectedUnmatchedDelta)
    }

    it("should merge two existing entities in one ohub group on exact match and preserve ohub id") {
      val integratedContactPersons = createDataset(contactPersonA1, contactPersonB1)
      val deltaContactPersons = createDataset(contactPersonB2, contactPersonA5)

      val expectedMatchedExact = Set(
        Result("100", isGoldenRecord = false, Some("BBB"), Some("Piet"), Some("piet@server.com")),
        Result("200", isGoldenRecord = true, Some("BBB"), Some("Piet"), Some("piet@server.com")),
        Result("103", isGoldenRecord = false, Some("AAA"), Some("Jan"), Some("jan@server.com"))
      )
      val expectedUnmatchedIntegrated = Set[Result]()
      val expectedUnmatchedDelta = Set[Result]()

      matchExactAndAssert(integratedContactPersons, deltaContactPersons, expectedMatchedExact, expectedUnmatchedIntegrated, expectedUnmatchedDelta)
    }

    it("should lose ohub id's when ") {
      val integratedContactPersons = createDataset(contactPersonA1, contactPersonA6)
      val deltaContactPersons = createDataset(contactPersonB2, contactPersonB3)

      val expectedMatchedExact = Set(
        Result("100", isGoldenRecord = false, Some(COPY_GENERATED), Some("Piet"), Some("piet@server.com")),
        Result("200", isGoldenRecord = false, Some(COPY_GENERATED), Some("Kees"), Some("kees@server.com"))) // you could argue to keep the ohub id for this one?
      val expectedUnmatchedIntegrated = Set[Result]()
      val expectedUnmatchedDelta = Set[Result]()

      matchExactAndAssert(integratedContactPersons, deltaContactPersons, expectedMatchedExact, expectedUnmatchedIntegrated, expectedUnmatchedDelta)
    }

    it("should lose only one ohub id ") {
      val integratedContactPersons = createDataset(contactPersonA1, contactPersonA6)
      val deltaContactPersons = createDataset(contactPersonA3, contactPersonB3)

      val expectedMatchedExact = Set(
        Result("100", isGoldenRecord = false, Some("AAA"), Some("Jansen"), Some("jan@server.com")),
        Result("200", isGoldenRecord = false, Some("COPY_GENERATED"), Some("Kees"), Some("kees@server.com")))
      val expectedUnmatchedIntegrated = Set[Result]()
      val expectedUnmatchedDelta = Set[Result]()

      matchExactAndAssert(integratedContactPersons, deltaContactPersons, expectedMatchedExact, expectedUnmatchedIntegrated, expectedUnmatchedDelta)
    }
  }

  private def matchExactAndAssert(
    integratedContactPersons: Dataset[ContactPerson],
    deltaContactPersons: Dataset[ContactPerson],
    expectedMatchedExact: Set[Result],
    expectedUnmatchedIntegrated: Set[Result],
    expectedUnmatchedDelta: Set[Result]
  ): Unit = {
    val (matchedExact, unmatchedIntegrated, unmatchedDelta) = ContactPersonIntegratedExactMatch.transform(
      spark, integratedContactPersons, deltaContactPersons
    )

    val actualMatchedExact = matchedExact.collect().toSeq.map(cp ⇒ cp.sourceEntityId -> cp).toMap
    val ohubIdsCopied = expectedMatchedExact.map { result ⇒
      if (result.ohubId.exists(_ == COPY_GENERATED)) {
        result.copy(ohubId = actualMatchedExact.get(result.sourceEntityId).flatMap(_.ohubId))
      } else {
        result
      }
    }

    assertResults(actualMatchedExact.values.toSeq.toDataset, ohubIdsCopied)
    assertResults(unmatchedIntegrated, expectedUnmatchedIntegrated)
    assertResults(unmatchedDelta, expectedUnmatchedDelta)
  }

  private def assertResults(actualDataSet: Dataset[ContactPerson], expectedResults: Set[Result]): Unit =
    createActualResultSet(actualDataSet) shouldBe expectedResults

  private def createActualResultSet(actualDataSet: Dataset[ContactPerson]): Set[Result] =
    actualDataSet.map(cp ⇒ Result(cp.sourceEntityId, cp.isGoldenRecord, cp.ohubId, cp.firstName, cp.emailAddress)).collect().toSet

  private def createDataset(contactPersons: ContactPerson*): Dataset[ContactPerson] = contactPersons.toDataset
}
