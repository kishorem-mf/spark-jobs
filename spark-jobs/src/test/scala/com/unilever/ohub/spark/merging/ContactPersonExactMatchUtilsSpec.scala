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
  private val contactPersonA3 = defaultContactPersonWithSourceEntityId("102").copy(ohubId = Some("BB1"), firstName = Some("Jansen"), emailAddress = None, mobileNumber = Some("12345"))
  private val contactPersonA4 = defaultContactPersonWithSourceEntityId("103").copy(ohubId = Some("AAA1"), firstName = Some("Jan"), emailAddress = Some("jan@server.com"), mobileNumber = Some("54321"))

  private val contactPersonB1 = defaultContactPersonWithSourceEntityId("200").copy(ohubId = Some("BBB"), firstName = Some("Piet"), emailAddress = Some("piet@server.com"), mobileNumber = Some("12345"))
  private val contactPersonB2 = defaultContactPersonWithSourceEntityId("201").copy(ohubId = None, firstName = Some("Piet"), emailAddress = Some("piet@server.com"), mobileNumber = Some("54321"))

  private val contactPersonA5 = defaultContactPersonWithSourceEntityId("301").copy(ohubId = Some("CCC"), firstName = Some("Kees"), emailAddress = None, mobileNumber = Some("98765"))
  private val contactPersonA6 = defaultContactPersonWithSourceEntityId("302").copy(ohubId = Some("CCC1"), firstName = Some("Kees"), emailAddress = None, mobileNumber = Some("98765"))
  private val contactPersonA7 = defaultContactPersonWithSourceEntityId("303").copy(ohubId = None, firstName = Some("Kees"), emailAddress = None, mobileNumber = Some("98765"))

  //private val contactPersonB3 = defaultContactPersonWithSourceEntityId("200").copy(ohubId = None, firstName = Some("Kees"), emailAddress = Some("kees@server.com"), mobileNumber = Some("12345"))
  //private val contactPersonB4 = defaultContactPersonWithSourceEntityId("200").copy(ohubId = Some("BBB"), firstName = Some("Piet"), emailAddress = None, mobileNumber = None)

  private val COPY_GENERATED = "COPY_GENERATED"

  describe("ContactPersonExactMatch.transform") {
    it("Use case : (1. Same Email, Mobile NULL , Same Group 2. Email NULL, Same Mobile, Same Group 3. Same Email, same Mobile, Same Group)") {

      /*
      Scenario 1: Same Email, Mobile NULL , Same Group
      Scenario 2: Same Email, same Mobile, Same Group
      Scenario 3: Same Email, Different Mobile, Same Group
      Scenario 4: Different Email, Different Mobile, Different Group
      Scenario 5: Different Email, Same Mobile, Same Group
      Scenario 6: Email NULL, Same Mobile, Same Group
      */

      val integratedContactPersons = createDataset(contactPersonA1, contactPersonA2)
      val deltaContactPersons = createDataset(contactPersonA3, contactPersonA4)

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

   /* it("should add a new entity to a existing group in integrated") {

      val integratedContactPersons = createDataset(contactPersonA1, contactPersonA2)
      val deltaContactPersons = createDataset(contactPersonA3, contactPersonA4)

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
    }*/


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
