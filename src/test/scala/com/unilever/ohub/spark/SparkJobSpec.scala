package com.unilever.ohub.spark

import java.sql.Timestamp
import java.util.UUID

import org.apache.spark.sql.{ Dataset, Encoder }
import org.scalatest.{ FunSpec, Matchers }
import SharedSparkSession.spark
import com.unilever.ohub.spark.domain.entity.Operator
import org.scalamock.scalatest.MockFactory

trait SparkJobSpec extends FunSpec with Matchers with MockFactory {

  protected implicit class ObjOps[T: Encoder](obj: T) {
    def toDataset: Dataset[T] = spark.createDataset(Seq(obj))
  }

  protected implicit class ObjsOps[T: Encoder](objs: Seq[T]) {
    def toDataset: Dataset[T] = spark.createDataset(objs)
  }

  // format: OFF
  protected val defaultOperatorRecord: Operator =
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
      dateCreated                 = new Timestamp(System.currentTimeMillis()),
      dateUpdated                 = new Timestamp(System.currentTimeMillis()),
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
  // format: ON

}
