package com.unilever.ohub.spark.merging

import java.sql.Timestamp

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.TestContactPersonsGolden

class ContactPersonCreatePerfectGoldenRecordSpecs extends SparkJobSpec with TestContactPersonsGolden {
  import spark.implicits._

  private val SUT = ContactPersonCreatePerfectGoldenRecord

  describe("ContactPersons create the Perfect golden record") {
    describe("full transform") {
      // Since calling withColumn for each column in ContactPersons is really slow due to the high amount of projects,
      // only 1 full transform is performed. (see https://issues.apache.org/jira/browse/SPARK-7276). On the cluster this
      // is no real deal-breaker a.t.m. Since merging performs fine for a full set of data.

      val cpMerge1 = defaultContactPersonGolden.copy(
        dateUpdated = Some(new Timestamp(1L)),
        firstName = Some("olderOp"),
        ohubId = Some("tcMerge"),
        dateCreated = Some(Timestamp.valueOf("2017-10-16 18:09:49")),
        sourceName = "EMAKINA",
        concatId = "whatever111",
        countryCode = "BR"
      )

      val cpMerge2 = defaultContactPersonGolden.copy(
        dateUpdated = None,
        firstName = Some("newerOp"),
        ohubId = Some("tcMerge"),
        dateCreated = Some(Timestamp.valueOf("2017-10-17 18:09:49")),
        sourceName = "FUZZIT",
        concatId = "whatever222",
        countryCode = "BR"
      )

      val cpMerge3 = defaultContactPersonGolden.copy(
        dateUpdated = None,
        firstName = Some("anotherOldOp"),
        ohubId = Some("tcMerge"),
        sourceName = "FUZZIT",
        concatId = "whatever333",
        countryCode = "BR"
      )

      val cpNoMerging = defaultContactPersonGolden.copy(
        dateUpdated = Some(new Timestamp(1L)),
        firstName = Some("newerOp1"),
        ohubId = Some("tcNoMerging"),
        sourceName = "EMAKINA"
      )

      val cpNull1 = defaultContactPersonGolden.copy(
        dateUpdated = Some(new Timestamp(1L)),
        firstName = None,
        ohubId = Some("tcNull")
      )

      val cpNull2 = defaultContactPersonGolden.copy(
        dateUpdated = None,
        firstName = Some("olderCp"),
        ohubId = Some("tcNull")
      )

      val cpInactive = defaultContactPersonGolden.copy(isActive = false, ohubId = Some("tcInactive"))

      val cpNewest1 = defaultContactPersonGolden.copy(
        dateUpdated = Some(new Timestamp(1L)),
        dateCreated = Some(new Timestamp(1L)),
        ohubUpdated = new Timestamp(1L),
        firstName = None,
        jobTitle = None,
        gender = Some("newest"),
        ohubId = Some("tcNewest"),
        hasEmailOptIn = Some(true),
        mobileDoubleOptInDate = Some(Timestamp.valueOf("2015-09-30 14:23:05.0"))
      )

      val cpNewest2 = defaultContactPersonGolden.copy(
        dateUpdated = None,
        dateCreated = Some(new Timestamp(1L)),
        ohubUpdated = new Timestamp(1L),
        firstName = None,
        jobTitle = Some("middle"),
        gender = Some("middle"),
        ohubId = Some("tcNewest")
      )

      val cpNewest3 = defaultContactPersonGolden.copy(
        dateUpdated = None,
        dateCreated = None,
        ohubUpdated = new Timestamp(1L),
        jobTitle = Some("oldest"),
        gender = Some("oldest"),
        ohubId = Some("tcNewest")
      )

      val cpSameDateUpdated1 = defaultContactPersonGolden.copy(
        dateUpdated = Some(new Timestamp(1561413600000L)), // 06/25/2019
        dateCreated = Some(new Timestamp(1560981600000L)), // 06/20/2019
        ohubUpdated = new Timestamp(1561845600000L), // 06/30/2019
        firstName = None,
        jobTitle = None,
        gender = Some("newest"),
        ohubId = Some("consentMerging"),

        // Email consent Fields
        hasEmailOptIn = null,
        emailOptInDate = Some(Timestamp.valueOf("2019-01-01 14:23:05.0")),
        hasEmailDoubleOptIn = null,
        emailDoubleOptInDate = Some(Timestamp.valueOf("2019-01-01 14:23:05.0")),
        hasEmailOptOut = null,

        // Mobile consent Fields
        hasMobileOptIn = Some(false),
        mobileOptInDate = null,
        hasMobileDoubleOptIn = null,
        mobileDoubleOptInDate = null,
        hasMobileOptOut = null,

        //DirectMail
        hasDirectMailOptIn = Some(false),
        hasDirectMailOptOut = Some(true),

        //Telemarketing
        hasTeleMarketingOptIn = Some(true),
        hasTeleMarketingOptOut = Some(false),

        // Fax
        hasFaxOptIn = Some(true),
        hasFaxOptOut = Some(false)
      )

      val cpSameDateUpdated2 = defaultContactPersonGolden.copy(
        dateUpdated = Some(new Timestamp(1561413600000L)), // 06/25/2019
        dateCreated = Some(new Timestamp(1560204000000L)), // 06/11/2019
        ohubUpdated = new Timestamp(1561845600000L), // 06/30/2019
        firstName = None,
        jobTitle = Some("middle"),
        gender = Some("middle"),
        ohubId = Some("consentMerging"),

        // Email consent Fields
        hasEmailOptIn = Some(true),
        emailOptInDate = Some(Timestamp.valueOf("2019-12-31 14:23:05.0")),
        hasEmailDoubleOptIn = Some(true),
        emailDoubleOptInDate = Some(Timestamp.valueOf("2019-12-31 14:23:05.0")),
        hasEmailOptOut = Some(true),

        // Mobile consent Fields
        hasMobileOptIn = Some(false),
        mobileOptInDate = Some(Timestamp.valueOf("2019-12-31 14:23:05.0")),
        hasMobileDoubleOptIn = Some(false),
        mobileDoubleOptInDate = Some(Timestamp.valueOf("2019-12-31 14:23:05.0")),
        hasMobileOptOut = Some(true),

        //DirectMail
        hasDirectMailOptIn = Some(true),
        hasDirectMailOptOut = Some(true),

        //Telemarketing
        hasTeleMarketingOptIn = Some(true),
        hasTeleMarketingOptOut = Some(true),

        // Fax
        hasFaxOptIn = Some(true),
        hasFaxOptOut = Some(true),

        socialNetworkName = Some("Facebook"),
        socialNetworkId = Some("1234")
      )

      val cpSameDateUpdated3 = defaultContactPersonGolden.copy(
        dateUpdated = Some(new Timestamp(1561413600000L)), // 06/25/2019
        dateCreated = Some(new Timestamp(1559340000000L)), // 06/01/2019
        ohubUpdated = new Timestamp(1561845600000L), // 06/30/2019
        firstName = Some("oldest"),
        jobTitle = Some("oldest"),
        gender = Some("oldest"),
        ohubId = Some("consentMerging"),

        // Email consent Fields
        hasEmailOptIn = Some(false),
        emailOptInDate = Some(Timestamp.valueOf("2019-06-15 14:23:05.0")),
        hasEmailDoubleOptIn = Some(false),
        emailDoubleOptInDate = Some(Timestamp.valueOf("2019-06-15 14:23:05.0")),
        hasEmailOptOut = Some(false),

        // Mobile consent Fields
        hasMobileOptIn = null,
        mobileOptInDate = null,
        hasMobileDoubleOptIn = null,
        mobileDoubleOptInDate = null,
        hasMobileOptOut = null,

        //DirectMail
        hasDirectMailOptIn = Some(true),
        hasDirectMailOptOut = Some(true),

        //Telemarketing
        hasTeleMarketingOptIn = Some(true),
        hasTeleMarketingOptOut = Some(true),

        // Fax
        hasFaxOptIn = Some(true),
        hasFaxOptOut = Some(true)
      )


      val input = Seq(cpMerge1, cpMerge2, cpMerge3, cpNull1, cpNull2, cpInactive, cpNewest1, cpNewest2, cpNewest3,
        cpSameDateUpdated1, cpSameDateUpdated2, cpSameDateUpdated3, cpNoMerging
      ).toDataset

      val result = SUT.transform(spark, input).collect

      it("should output 1 record for each group with active ContactPersons") {
        result.length shouldBe (5)
      }

      it("should calculate the concatid as Merged Concat ID") {
        val tcResult = result.filter(_.ohubId == Some("tcMerge"))
        tcResult.head.concatId shouldBe "BR~OHUB~tcMerge"
      }

      it("should not output inactive groups") {
        val tcResult = result.filter(_.ohubId == Some("tcInactive"))
        tcResult.length shouldBe 0
      }

      it("should merge 2 records from the same group") {
        val tcResult = result.filter(_.ohubId == Some("tcMerge"))
        tcResult.length shouldBe 1
        tcResult.head.firstName shouldBe cpMerge2.firstName
        tcResult.head.sourceName shouldBe "EMAKINA,FUZZIT"
      }

      it("should not merge sources when there is only 1 group") {
        val tcResult = result.filter(_.ohubId == Some("tcNoMerging"))
        tcResult.length shouldBe 1
        tcResult.head.sourceName shouldBe "EMAKINA"
      }

      it("should merge groups based on multiple date columns") {
        val tcResult = result.filter(_.ohubId == Some("tcNewest"))
        tcResult.length shouldBe 1
        tcResult.head.firstName shouldBe cpNewest3.firstName
        tcResult.head.jobTitle shouldBe cpNewest2.jobTitle
        tcResult.head.gender shouldBe cpNewest1.gender
      }

      it("Should take the most recent for every and merge properly the consent by channel") {
        val tcResult = result.filter(_.ohubId == Some("consentMerging"))

        tcResult.length shouldBe 1
        tcResult.head.dateUpdated shouldBe cpSameDateUpdated1.dateUpdated
        tcResult.head.firstName shouldBe cpSameDateUpdated3.firstName
        tcResult.head.jobTitle shouldBe cpSameDateUpdated2.jobTitle
        tcResult.head.gender shouldBe cpSameDateUpdated1.gender
        tcResult.head.dateCreated shouldBe cpSameDateUpdated3.dateCreated

        // Email consent
        tcResult.head.hasEmailOptIn shouldBe cpSameDateUpdated2.hasEmailOptIn
        tcResult.head.emailOptInDate shouldBe cpSameDateUpdated2.emailOptInDate
        tcResult.head.hasEmailDoubleOptIn shouldBe cpSameDateUpdated2.hasEmailDoubleOptIn
        tcResult.head.emailDoubleOptInDate shouldBe cpSameDateUpdated2.emailDoubleOptInDate
        tcResult.head.hasEmailOptOut shouldBe cpSameDateUpdated2.hasEmailOptOut

        // Mobile consent
        tcResult.head.hasMobileOptIn shouldBe cpSameDateUpdated2.hasMobileOptIn
        tcResult.head.mobileOptInDate shouldBe cpSameDateUpdated2.mobileOptInDate
        tcResult.head.hasMobileDoubleOptIn shouldBe cpSameDateUpdated2.hasMobileDoubleOptIn
        tcResult.head.mobileDoubleOptInDate shouldBe cpSameDateUpdated2.mobileDoubleOptInDate
        tcResult.head.hasMobileOptOut shouldBe cpSameDateUpdated2.hasMobileOptOut

        // TeleMarketing
        tcResult.head.hasTeleMarketingOptIn shouldBe cpSameDateUpdated1.hasTeleMarketingOptIn
        tcResult.head.hasTeleMarketingOptOut shouldBe cpSameDateUpdated1.hasTeleMarketingOptOut

        // socialNetworkName & Id
        tcResult.head.socialNetworkName shouldBe cpSameDateUpdated2.socialNetworkName
        tcResult.head.socialNetworkId shouldBe cpSameDateUpdated2.socialNetworkId
      }
    }
  }
}
