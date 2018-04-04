package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp
import java.util.UUID

import org.scalatest.{Matchers, WordSpec}

class OperatorSpec extends WordSpec with Matchers {

  "Operator" should {
    "be created correctly (no exception is thrown)" when {
      "only valid data is provided" in {
        val operator = Operator(
          concatId                    = "concat-id",
          countryCode                 = "country-code",
          isActive                    = true,
          isGoldenRecord              = false,
          ohubId                     = Some(UUID.randomUUID().toString),
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
          dateUpdated                 = Some(new Timestamp(System.currentTimeMillis())),
          daysOpen                    = Some(4),
          distributorCustomerNumber   = Some("distributor-customer-number"),
          distributorName             = Some("distributor-name"),
          distributorOperatorId       = None,
          emailAddress                = Some("email-address@some-server.com"),
          faxNumber                   = Some("+31123456789"),
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

        operator.name shouldBe "operator-name"
      }
    }
  }
}
