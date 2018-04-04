package com.unilever.ohub.spark.deduplicate

import java.sql.Timestamp
import java.util.UUID

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.SharedSparkSession.spark

class OperatorDeduplicationTest extends SparkJobSpec {

  import spark.implicits._

  private val defaultOperatorRecord: Operator =
    Operator(
      concatId                    = UUID.randomUUID().toString,
      countryCode                 = "country-code",
      isActive                    = true,
      isGoldenRecord              = false,
      ohubId                      = None,
      name                        = "operator-name",
      sourceEntityId              = "source-entity-id",
      sourceName                  = "source-name",
      ohubCreated                 = new Timestamp(System.currentTimeMillis()),
      ohubUpdated                 = new Timestamp(System.currentTimeMillis()),
      averagePrice                = None,
      chainId                     = None,
      chainName                   = None,
      channel                     = None,
      city                        = None,
      cookingConvenienceLevel     = None,
      countryName                 = None,
      customerType                = None,
      dateCreated                 = None,
      dateUpdated                 = Some(new Timestamp(System.currentTimeMillis())),
      daysOpen                    = None,
      distributorCustomerNumber   = None,
      distributorName             = None,
      distributorOperatorId       = None,
      emailAddress                = None,
      faxNumber                   = None,
      hasDirectMailOptIn          = None,
      hasDirectMailOptOut         = None,
      hasEmailOptIn               = None,
      hasEmailOptOut              = None,
      hasFaxOptIn                 = None,
      hasFaxOptOut                = None,
      hasGeneralOptOut            = None,
      hasMobileOptIn              = None,
      hasMobileOptOut             = None,
      hasTelemarketingOptIn       = None,
      hasTelemarketingOptOut      = None,
      houseNumber                 = None,
      houseNumberExtension        = None,
      isNotRecalculatingOtm       = None,
      isOpenOnFriday              = None,
      isOpenOnMonday              = None,
      isOpenOnSaturday            = None,
      isOpenOnSunday              = None,
      isOpenOnThursday            = None,
      isOpenOnTuesday             = None,
      isOpenOnWednesday           = None,
      isPrivateHousehold          = None,
      kitchenType                 = None,
      mobileNumber                = None,
      netPromoterScore            = None,
      oldIntegrationId            = None,
      otm                         = None,
      otmEnteredBy                = None,
      phoneNumber                 = None,
      region                      = None,
      salesRepresentative         = None,
      state                       = None,
      street                      = None,
      subChannel                  = None,
      totalDishes                 = None,
      totalLocations              = None,
      totalStaff                  = None,
      vat                         = None,
      webUpdaterId                = None,
      weeksClosed                 = None,
      zipCode                     = None,
      ingestionErrors             = Map()
    )

  describe("deduplication") {

    it("should return integrated if daily is empty") {
      val integrated = defaultOperatorRecord.toDataset

      val daily = spark.emptyDataset[Operator]

      val (updatedDs, dailyNewDs) = OperatorDeduplication.transform(spark, integrated, daily)

      val updated = updatedDs.collect
      val dailyNew = dailyNewDs.collect
      assert(updated.length === 1)
      assert(dailyNew.length === 0)
    }

    it("should find no duplicates if none are there") {
      val integrated = Seq(
        defaultOperatorRecord.copy(concatId = "a"),
        defaultOperatorRecord.copy(concatId = "b")
      ).toDataset

      val daily = defaultOperatorRecord.copy(concatId = "c").toDataset

      val (updatedDs, dailyNewDs) = OperatorDeduplication.transform(spark, integrated, daily)

      val updated = updatedDs.collect
      val dailyNew = dailyNewDs.collect
      assert(updated.length === 2)
      assert(dailyNew.length === 1)

      updated.map(_.concatId) should contain theSameElementsAs Seq("a", "b")
      assert(dailyNew.head.concatId === "c")
    }

    it("should return a deduplicated dataset if there are duplicates") {
      val integrated = Seq(
        defaultOperatorRecord.copy(concatId = "a"),
        defaultOperatorRecord.copy(concatId = "b")
      ).toDataset
      val daily = defaultOperatorRecord.copy(concatId = "a").toDataset

      val (updatedDs, dailyNewDs) = OperatorDeduplication.transform(spark, integrated, daily)

      val updated = updatedDs.collect
      val dailyNew = dailyNewDs.collect
      assert(updated.length === 2)
      assert(dailyNew.length === 0)

      updated.map(_.concatId) should contain theSameElementsAs Seq("a", "b")
    }

    it("return a deduplicated dataset with the newest record if there are duplicates") {
      val integrated = Seq(
        defaultOperatorRecord.copy(concatId = "a", dateUpdated = Some(Timestamp.valueOf("2017-05-25 12:00:00"))),
        defaultOperatorRecord.copy(concatId = "b")
      ).toDataset

      val newTimestamp = Some(Timestamp.valueOf("2017-06-25 12:00:00"))
      val daily = defaultOperatorRecord.copy(concatId = "a", dateUpdated = newTimestamp).toDataset

      val (updatedDs, dailyNewDs) = OperatorDeduplication.transform(spark, integrated, daily)

      val updated = updatedDs.collect
      val dailyNew = dailyNewDs.collect
      assert(updated.length === 2)
      assert(dailyNew.length === 0)

      updated.map(_.concatId) should contain theSameElementsAs Seq("a", "b")
      assert(updated.find(_.concatId == "a").get.dateUpdated === newTimestamp)
    }
  }
}
