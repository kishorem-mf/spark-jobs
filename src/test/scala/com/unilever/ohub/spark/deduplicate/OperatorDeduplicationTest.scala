package com.unilever.ohub.spark.deduplicate

import java.sql.Timestamp
import java.util.UUID

import com.unilever.ohub.spark.domain.entity.Operator
import org.scalatest.FlatSpec
import com.unilever.ohub.spark.SharedSparkSession.spark

class OperatorDeduplicationTest extends FlatSpec {

  import spark.implicits._

  private def generateOperator(concatId:String = UUID.randomUUID().toString,
                               dateUpdated: Timestamp = new Timestamp(System.currentTimeMillis())
                              ): Operator = {
    Operator(
      concatId                    = concatId,
      countryCode                 = "country-code",
      isActive                    = true,
      isGoldenRecord              = false,
      groupId                     = Some(UUID.randomUUID().toString),
      name                        = "operator-name",
      sourceEntityId              = "source-entity-id",
      sourceName                  = "source-name",
      ohubCreated                 = new Timestamp(System.currentTimeMillis()),
      ohubUpdated                 = new Timestamp(System.currentTimeMillis()),
      averagePrice                = Some(BigDecimal(12345)),
      chainId                     = Some("chain-id"),
      chainName                   = Some("chain-name"),
      channel                     = Some("channel"),
      city                        = Some("city"),
      cookingConvenienceLevel     = Some("cooking-convenience-level"),
      countryName                 = Some("country-name"),
      customerType                = Some("customer-type"),
      dateCreated                 = Some(new Timestamp(System.currentTimeMillis())),
      dateUpdated                 = Some(dateUpdated),
      daysOpen                    = Some(4),
      distributorCustomerNumber   = Some("distributor-customer-number"),
      distributorName             = Some("distributor-name"),
      distributorOperatorId       = None,
      emailAddress                = Some("email-address@some-server.com"),
      faxNumber                   = Some("+31123456789"),
      germanChainId               = None,
      germanChainName             = None,
      hasDirectMailOptIn          = Some(true),
      hasDirectMailOptOut         = Some(false),
      hasEmailOptIn               = Some(true),
      hasEmailOptOut              = Some(false),
      hasFaxOptIn                 = Some(true),
      hasFaxOptOut                = Some(false),
      hasGeneralOptOut            = Some(false),
      hasMobileOptIn              = Some(true),
      hasMobileOptOut             = Some(false),
      hasTelemarketingOptIn       = Some(true),
      hasTelemarketingOptOut      = Some(false),
      houseNumber                 = Some("12"),
      houseNumberExtension        = None,
      isNotRecalculatingOtm       = Some(true),
      isOpenOnFriday              = Some(true),
      isOpenOnMonday              = Some(false),
      isOpenOnSaturday            = Some(true),
      isOpenOnSunday              = Some(false),
      isOpenOnThursday            = Some(true),
      isOpenOnTuesday             = Some(true),
      isOpenOnWednesday           = Some(true),
      isPrivateHousehold          = Some(false),
      kitchenType                 = Some("kitchen-type"),
      mobileNumber                = Some("+31612345678"),
      netPromoterScore            = Some(BigDecimal(75)),
      oldIntegrationId            = Some("old-integration-id"),
      otm                         = Some("otm"),
      otmEnteredBy                = Some("otm-entered-by"),
      phoneNumber                 = Some("+31123456789"),
      region                      = Some("region"),
      salesRepresentative         = Some("sales-representative"),
      state                       = Some("state"),
      street                      = Some("street"),
      subChannel                  = Some("sub-channel"),
      totalDishes                 = Some(150),
      totalLocations              = Some(25),
      totalStaff                  = Some(105),
      vat                         = Some("vat"),
      webUpdaterId                = Some("web-updater-id"),
      weeksClosed                 = Some(2),
      zipCode                     = Some("1234 AB"),
      ingestionErrors             = Map()
    )
  }
  behavior of "deduplication"

  it should "find no duplicates if none are there" in {
    val integrated = spark.createDataset(Seq(
      generateOperator("a"),
      generateOperator("b")
    ))
    val daily = spark.createDataset(Seq(
      generateOperator("c")
    ))

    val (updatedDs, dailyNewDs) = OperatorDeduplication.transform(spark, integrated, daily)

    val updated = updatedDs.collect
    val dailyNew = dailyNewDs.collect
    assert(updated.length === 2)
    assert(dailyNew.length === 1)

    assert(updated.map(_.concatId) === Seq("a", "b"))
    assert(dailyNew.map(_.concatId) === Seq("c"))
  }
}
