package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.{ ContactPerson, TestContactPersons }
import org.apache.spark.sql.{ Dataset, SparkSession }

case class ResultExactContactPerson(sourceEntityId: String, ohubId: Option[String], firstName: Option[String], emailAddress: Option[String], mobileNumber: Option[String])

class ContactPersonExactMatchUtilsSpec extends SparkJobSpec with TestContactPersons {
  import spark.implicits._

  lazy implicit val implicitSpark: SparkSession = spark

  private val contactPersonA1 = defaultContactPersonWithSourceEntityId("100").copy(ohubId = Some("AAA"), firstName = Some("Jan"), emailAddress = Some("jan@server.com"), mobileNumber = Some("12345"))
  private val contactPersonA2 = defaultContactPersonWithSourceEntityId("101").copy(ohubId = Some("AAA"), firstName = Some("Jan"), emailAddress = Some("jan@server.com"), mobileNumber = None)
  private val contactPersonA3 = defaultContactPersonWithSourceEntityId("102").copy(ohubId = Some("AAA2"), firstName = Some("unknown"), emailAddress = None, mobileNumber = Some("12345"))
  private val contactPersonA4 = defaultContactPersonWithSourceEntityId("103").copy(ohubId = Some("AAA1"), firstName = Some("Jan"), emailAddress = Some("jan@server.com"), mobileNumber = Some("54321"))

  private val contactPersonA8 = defaultContactPersonWithSourceEntityId("104").copy(ohubId = Some("AAA1"), firstName = Some("Jan"), emailAddress = Some("jan@server.com"), mobileNumber = Some("12345"))
  private val contactPersonA9 = defaultContactPersonWithSourceEntityId("105").copy(ohubId = Some("BBB1"), firstName = Some("Jan"), emailAddress = Some("jan@server.com"), mobileNumber = Some("12345"))
  private val contactPersonA10 = defaultContactPersonWithSourceEntityId("106").copy(ohubId = Some("BBB"), firstName = Some("Jan"), emailAddress = Some("jan@server.com"), mobileNumber = Some("12345"))
  private val contactPersonA11 = defaultContactPersonWithSourceEntityId("107").copy(ohubId = Some("CCC"), firstName = Some("Jan"), emailAddress = Some("jan@server.com"), mobileNumber = Some("12345"))

  private val contactPerson12 = defaultContactPersonWithSourceEntityId("108").copy(ohubId = Some("AAA"), firstName = Some("Jan"), emailAddress = Some("jan@server.com"), mobileNumber = Some("12345"))
  private val contactPerson13 = defaultContactPersonWithSourceEntityId("109").copy(ohubId = Some("BBB1"), firstName = Some("Jan1"), emailAddress = Some("Jan1@server.com"), mobileNumber = Some("1234"))
  private val contactPersonA14 = defaultContactPersonWithSourceEntityId("110").copy(ohubId = Some("BBB"), firstName = Some("Jan2"), emailAddress = Some("Jan2@server.com"), mobileNumber = Some("123456"))
  private val contactPersonA15 = defaultContactPersonWithSourceEntityId("111").copy(ohubId = Some("CCC"), firstName = Some("Jan3"), emailAddress = Some("JAN@server.com"), mobileNumber = Some("123457"))

  private val contactPerson16 = defaultContactPersonWithSourceEntityId("112").copy(ohubId = Some("AAA"), firstName = Some("Jan"), emailAddress = Some("jan@server.com"), mobileNumber = Some("12345"))
  private val contactPerson17 = defaultContactPersonWithSourceEntityId("113").copy(ohubId = Some("BBB1"), firstName = Some("Jan1"), emailAddress = Some("Jan1@server.com"), mobileNumber = Some("12345"))
  private val contactPersonA18 = defaultContactPersonWithSourceEntityId("114").copy(ohubId = Some("BBB"), firstName = Some("Jan2"), emailAddress = Some("Jan2@server.com"), mobileNumber = Some("12345"))
  private val contactPersonA19 = defaultContactPersonWithSourceEntityId("115").copy(ohubId = Some("CCC"), firstName = Some("Jan3"), emailAddress = Some("Jan3@server.com"), mobileNumber = Some("12345"))

  private val COPY_GENERATED = "COPY_GENERATED"

     /*
     Scenario 1: Same Email, Mobile NULL , Same Group(D)
     Scenario 2: Same Email, same Mobile, Same Group(D)
     Scenario 3: Same Email, Different Mobile, Same Group(D)
     Scenario 4: Different Email, Different Mobile, Different Group(D)
     Scenario 5: Different Email, Same Mobile, Different Group
     Scenario 6: Email NULL, Same Mobile, Same Group(D)
     */
  describe("ContactPersonExactMatch.transform") {
    it("Use case : (1. Email NULL, Same Mobile, Same Group)(2. Same Email, Different Mobile, Same Group)(3. Same Email, Mobile NULL , Same Group)") {

      val integratedContactPersons = createDataset(contactPersonA1, contactPersonA2)
      val deltaContactPersons = createDataset(contactPersonA3,contactPersonA4)

      val expectedMatchedExact = Set(
        ResultContactPerson("100", Some("AAA"), Some("Jan"), Some("jan@server.com"), Some("12345")),
        ResultContactPerson("200", Some("BBB"), Some("Piet"), Some("piet@server.com"), Some("12345"))
      )
      val expectedUnmatchedIntegrated = Set(
        ResultContactPerson("100", Some("AAA"), Some("Jan"), Some("jan@server.com"), Some("12345")),
        ResultContactPerson("200", Some("BBB"), Some("Piet"), Some("piet@server.com"), Some("12345"))
      )

      val expectedUnmatchedDelta = Set[ResultContactPerson]()

      matchExactAndAssert(integratedContactPersons, deltaContactPersons, expectedMatchedExact, expectedUnmatchedIntegrated, expectedUnmatchedDelta)
    }

    it("Same Email, same Mobile, Same Group") {

      val integratedContactPersons = createDataset(contactPersonA8, contactPersonA9)
      val deltaContactPersons = createDataset(contactPersonA10, contactPersonA11)

      val expectedMatchedExact = Set[ResultContactPerson](
        /*ResultContactPerson("100", Some("AAA"), Some("Jan"), Some("jan@server.com"), Some("12345")),
        ResultContactPerson("200", Some("BBB"), Some("Piet"), Some("piet@server.com"), Some("12345"))*/
      )
      val expectedUnmatchedIntegrated = Set[ResultContactPerson](
      /*  ResultContactPerson("100", Some("AAA"), Some("Jan"), Some("jan@server.com"), Some("12345")),
        ResultContactPerson("200", Some("BBB"), Some("Piet"), Some("piet@server.com"), Some("12345"))*/
      )

      val expectedUnmatchedDelta = Set[ResultContactPerson]()

      matchExactAndAssert(integratedContactPersons, deltaContactPersons, expectedMatchedExact, expectedUnmatchedIntegrated, expectedUnmatchedDelta)
    }

    it("Different Email, Different Mobile, Different Group") {

      val integratedContactPersons = createDataset(contactPerson12, contactPerson13)
      val deltaContactPersons = createDataset(contactPersonA14, contactPersonA15)

      val expectedMatchedExact = Set[ResultContactPerson](
        /*ResultContactPerson("100", Some("AAA"), Some("Jan"), Some("jan@server.com"), Some("12345")),
        ResultContactPerson("200", Some("BBB"), Some("Piet"), Some("piet@server.com"), Some("12345"))*/
      )
      val expectedUnmatchedIntegrated = Set[ResultContactPerson](
        /*  ResultContactPerson("100", Some("AAA"), Some("Jan"), Some("jan@server.com"), Some("12345")),
          ResultContactPerson("200", Some("BBB"), Some("Piet"), Some("piet@server.com"), Some("12345"))*/
      )

      val expectedUnmatchedDelta = Set[ResultContactPerson]()

      matchExactAndAssert(integratedContactPersons, deltaContactPersons, expectedMatchedExact, expectedUnmatchedIntegrated, expectedUnmatchedDelta)
    }

    it("Different Email, Same Mobile, Different Group") {

      val integratedContactPersons = createDataset(contactPerson16, contactPerson17)
      val deltaContactPersons = createDataset(contactPersonA18, contactPersonA19)

      val expectedMatchedExact = Set[ResultContactPerson](
        /*ResultContactPerson("100", Some("AAA"), Some("Jan"), Some("jan@server.com"), Some("12345")),
        ResultContactPerson("200", Some("BBB"), Some("Piet"), Some("piet@server.com"), Some("12345"))*/
      )
      val expectedUnmatchedIntegrated = Set[ResultContactPerson](
        /*  ResultContactPerson("100", Some("AAA"), Some("Jan"), Some("jan@server.com"), Some("12345")),
          ResultContactPerson("200", Some("BBB"), Some("Piet"), Some("piet@server.com"), Some("12345"))*/
      )

      val expectedUnmatchedDelta = Set[ResultContactPerson]()

      matchExactAndAssert(integratedContactPersons, deltaContactPersons, expectedMatchedExact, expectedUnmatchedIntegrated, expectedUnmatchedDelta)
    }


  }

  private def matchExactAndAssert(
                                   integratedContactPersons: Dataset[ContactPerson],
                                   deltaContactPersons: Dataset[ContactPerson],
                                   expectedMatchedExact: Set[ResultContactPerson],
                                   expectedUnmatchedIntegrated: Set[ResultContactPerson],
                                   expectedUnmatchedDelta: Set[ResultContactPerson]
                                 ): Dataset[ContactPerson] = {

    val matchedExact = ContactPersonExactMatchUtils.determineExactMatchOnEmailAndMobile(
      integratedContactPersons, deltaContactPersons
    )

    println("matchedExact")
    matchedExact.select("concatId", "ohubId","emailAddress","mobileNumber").show()
    println("===============================================================")

    val actualMatchedExact = convertDataSetToMap(matchedExact)
    //val matchedOhubIdsCopied = copyOhubIds(expectedMatchedExact, actualMatchedExact)

  /*  val actualUnmatchedIntegrated = convertDataSetToMap(unmatchedIntegrated)
    val unmatchedIntegOhubIdsCopied = copyOhubIds(expectedUnmatchedIntegrated, actualUnmatchedIntegrated)

    assertResults("unmatchedIntegrated", actualUnmatchedIntegrated.values.toSeq.toDataset, unmatchedIntegOhubIdsCopied)
    assertResults("unmatchedDelta", unmatchedDelta, expectedUnmatchedDelta)
    assertResults("matched", actualMatchedExact.values.toSeq.toDataset, matchedOhubIdsCopied)
*/
    matchedExact
  }

  private def assertResults(title: String, actualDataSet: Dataset[ContactPerson], expectedResults: Set[ResultExactContactPerson]): Unit =
    withClue(s"$title:")(createActualResultSet(actualDataSet) shouldBe expectedResults)

  private def createActualResultSet(actualDataSet: Dataset[ContactPerson]): Set[ResultExactContactPerson] =
    actualDataSet.map(cp ⇒ ResultExactContactPerson(cp.sourceEntityId, cp.ohubId, cp.firstName, cp.emailAddress, cp.mobileNumber)).collect().toSet

  private def createDataset(contactPersons: ContactPerson*): Dataset[ContactPerson] = contactPersons.toDataset

  def copyOhubIds(expectedSet: Set[ResultExactContactPerson], actualSet: Map[String, ContactPerson]) = {
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
