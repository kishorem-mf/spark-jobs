package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.{ContactPerson, TestContactPersons}
import org.apache.spark.sql.{Dataset, SparkSession}

case class SampleContactPerson(
          sourceEntityId: String,
          ohubId: Option[String],
          houseNumber: Option[String],
          houseNumberExtension: Option[String],
          city: Option[String],
          street: Option[String],
          zipCode: Option[String])

class ContactPersonExactLocationSpec extends SparkJobSpec with TestContactPersons {

  import spark.implicits._

  private val COPY_GENERATED = "COPY_GENERATED"

  lazy implicit val implicitSpark: SparkSession = spark

  describe("Test exact matching based on location") {
    it("same record in delta and integrated") {
      val contactPersonA1 = defaultContactPersonWithSourceEntityId("100").copy(
        ohubId = Some("AAA"),
        firstName = Some("Jan"),
        city = Some("Rotterdam"),
        street = Some("Weena"),
        houseNumber = Some("3113"),
        houseNumberExtension = None,
        zipCode = Some("3124"),
        mobileNumber = None,
        emailAddress = None)

      val contactPersonA2 = defaultContactPersonWithSourceEntityId("100").copy(
        ohubId = Some("AAA"),
        firstName = Some("Jan"),
        city = Some("Rotterdam"),
        street = Some("Weena"),
        houseNumber = Some("3113"),
        houseNumberExtension = None,
        zipCode = Some("3124"),
        mobileNumber = None,
        emailAddress = None)
      val integratedContactPersons = createDataset(contactPersonA1)
      val deltaContactPersons =  createDataset(contactPersonA2)

      val expectedMatchedExact = Set(
        SampleContactPerson("100", Some("AAA"), Some("3113"), None, Some("Weena"), Some("Rotterdam"),Some("3124"))
      )
      val expectedUnmatchedIntegrated = Set(
        SampleContactPerson("100", Some("AAA"), Some("3113"), None, Some("Weena"), Some("Rotterdam"),Some("3124"))
      )

      val expectedUnmatchedDelta = Set[SampleContactPerson]()
      matchExactAndAssert(integratedContactPersons, deltaContactPersons, expectedMatchedExact, expectedUnmatchedIntegrated, expectedUnmatchedDelta)
    }

    it("different ohubId having same location in delta and integrated") {
      val contactPersonA1 = defaultContactPersonWithSourceEntityId("100").copy(
        ohubId = Some("AAA"),
        firstName = Some("Jan"),
        city = Some("Rotterdam"),
        street = Some("Weena"),
        houseNumber = Some("3113"),
        houseNumberExtension = None,
        zipCode = Some("3124"),
        mobileNumber = None,
        emailAddress = None)

      val contactPersonA2 = defaultContactPersonWithSourceEntityId("101").copy(
        ohubId = Some("BBB"),
        firstName = Some("Jan"),
        city = Some("Rotterdam"),
        street = Some("Weena"),
        houseNumber = Some("3113"),
        houseNumberExtension = None,
        zipCode = Some("3124"),
        mobileNumber = None,
        emailAddress = None)

      val integratedContactPersons = createDataset(contactPersonA1)
      val deltaContactPersons =  createDataset(contactPersonA2)

      val expectedMatchedExact = Set(
        SampleContactPerson("100", Some("BBB"), Some("3113"), None, Some("Weena"), Some("Rotterdam"),Some("3124")),
        SampleContactPerson("101", Some("BBB"), Some("3113"), None, Some("Weena"), Some("Rotterdam"),Some("3124"))
      )
      val expectedUnmatchedIntegrated = Set(
        SampleContactPerson("100", Some("BBB"), Some("3113"), None, Some("Weena"), Some("Rotterdam"),Some("3124"))
      )

      val expectedUnmatchedDelta = Set[SampleContactPerson]()
      matchExactAndAssert(integratedContactPersons, deltaContactPersons, expectedMatchedExact, expectedUnmatchedIntegrated, expectedUnmatchedDelta)
    }

    it("same record in delta and integrated to test on case sensitivity") {
      val contactPersonA1 = defaultContactPersonWithSourceEntityId("100").copy(
        ohubId = Some("AAA"),
        firstName = Some("Jan"),
        city = Some("ROTTERDAM"),
        street = Some("Weena"),
        houseNumber = Some("3113"),
        houseNumberExtension = None,
        zipCode = Some("3124"),
        mobileNumber = None,
        emailAddress = None)

      val contactPersonA2 = defaultContactPersonWithSourceEntityId("101").copy(
        ohubId = Some("AAA"),
        firstName = Some("Jan"),
        city = Some("Rotterdam"),
        street = Some("WEENA"),
        houseNumber = Some("3113"),
        houseNumberExtension = None,
        zipCode = Some("3124"),
        mobileNumber = None,
        emailAddress = None)
      val integratedContactPersons = createDataset(contactPersonA1)
      val deltaContactPersons =  createDataset(contactPersonA2)

      val expectedMatchedExact = Set(
        SampleContactPerson("100", Some("AAA"), Some("3113"), None, Some("Weena"), Some("ROTTERDAM"),Some("3124")),
        SampleContactPerson("101", Some("AAA"), Some("3113"), None, Some("WEENA"), Some("Rotterdam"),Some("3124"))
      )
      val expectedUnmatchedIntegrated = Set(
        SampleContactPerson("100", Some("AAA"), Some("3113"), None, Some("Weena"), Some("ROTTERDAM"),Some("3124"))
      )

      val expectedUnmatchedDelta = Set[SampleContactPerson]()
      matchExactAndAssert(integratedContactPersons, deltaContactPersons, expectedMatchedExact, expectedUnmatchedIntegrated, expectedUnmatchedDelta)
    }
  }


  private def matchExactAndAssert(
       integratedContactPersons: Dataset[ContactPerson],
       deltaContactPersons: Dataset[ContactPerson],
       expectedMatchedExact: Set[SampleContactPerson],
       expectedUnmatchedIntegrated: Set[SampleContactPerson],
       expectedUnmatchedDelta: Set[SampleContactPerson]
     ): Dataset[ContactPerson] = {

    val (matchedExact, unmatchedIntegrated, unmatchedDelta) = ContactPersonIntegratedExactMatch.transform(
      integratedContactPersons, deltaContactPersons
    )

    val actualMatchedExact = convertDataSetToMap(matchedExact)
    val matchedOhubIdsCopied = copyOhubIds(expectedMatchedExact, actualMatchedExact)

    val actualUnmatchedIntegrated = convertDataSetToMap(unmatchedIntegrated)
    val unmatchedIntegOhubIdsCopied = copyOhubIds(expectedUnmatchedIntegrated, actualUnmatchedIntegrated)

    assertResults("unmatchedIntegrated", actualUnmatchedIntegrated.values.toSeq.toDataset, unmatchedIntegOhubIdsCopied)
    assertResults("unmatchedDelta", unmatchedDelta, expectedUnmatchedDelta)
    assertResults("matched", actualMatchedExact.values.toSeq.toDataset, matchedOhubIdsCopied)

    matchedExact
  }

  private def assertResults(title: String, actualDataSet: Dataset[ContactPerson], expectedResults: Set[SampleContactPerson]): Unit =
    withClue(s"$title:")(createActualResultSet(actualDataSet) shouldBe expectedResults)

  private def createActualResultSet(actualDataSet: Dataset[ContactPerson]): Set[SampleContactPerson] =
    actualDataSet.map(cp ⇒ SampleContactPerson(cp.sourceEntityId, cp.ohubId, cp.houseNumber, cp.houseNumberExtension, cp.street, cp.city, cp.zipCode)).collect().toSet

  private def createDataset(contactPersons: ContactPerson*): Dataset[ContactPerson] = contactPersons.toDataset

  def copyOhubIds(expectedSet: Set[SampleContactPerson], actualSet: Map[String, ContactPerson]) = {
    expectedSet.map { result ⇒
      if (result.ohubId.contains(COPY_GENERATED)) {
        result.copy(ohubId = actualSet.get(result.sourceEntityId).flatMap(_.ohubId))
      } else {
        result
      }
    }
  }

  def convertDataSetToMap(dataSet: Dataset[ContactPerson]) =
    dataSet.collect().toSeq.map(cp ⇒ cp.sourceEntityId -> cp).toMap
}
