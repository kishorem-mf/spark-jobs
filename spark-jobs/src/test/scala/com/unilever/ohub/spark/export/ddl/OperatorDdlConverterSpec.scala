package com.unilever.ohub.spark.export.ddl

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.TestOperatorsGolden
import com.unilever.ohub.spark.export.ddl.model.DdlOperator

class OperatorDdlConverterSpec extends SparkJobSpec with TestOperatorsGolden {


  val SUT = OperatorDdlConverter
  val operatorToConvert = defaultOperatorGolden.copy(ohubId = Some("12345"),
    globalChannel = Some("globalChannel"), customerHierarchyLevel3 = Some("customerHierarchyLevel3"),
    customerHierarchyLevel4 = Some("customerHierarchyLevel4"),
    customerHierarchyLevel5 = Some("customerHierarchyLevel5"),
    customerHierarchyLevel7 = Some("customerHierarchyLevel7"), latitude = Some("latitude"),
    longitude = Some("longitude"))

  describe("operator ddl converter") {
    it("should convert a domain operator correctly into an ddl operator") {

      val result = SUT.convert(operatorToConvert)

      val expectedDdlOperator = DdlOperator(
        `AFH Customer Golden ID` = "12345",
        `CRM Account ID` = "",
        `Customer SAP concat ID` = "source-entity-id",
        Channel = "",
        Division = "",
        SalesOrgID = "",
        ParentSourceCustomerCode = "",
        `Closing Time Working Day` = "",
        `Opening Time Working Day` = "",
        `Preferred Visit Days` = "",
        `Preferred Visit Start Time` = "",
        `Preferred Visit End Time` = "",
        `Preferred Delivery Day/s` = "",
        `Preferred Visit week of Month` = "",
        `Customer Name` = "operator-name",
        `Customer Name 2` = "",
        `Address Line 1` = "street",
        City = "city",
        State = "state",
        `Postal Code` = "1234 AB",
        Country = "country-name",
        Region = "region",
        `Phone Number 1` = "+31123456789",
        Fax = "+31123456789",
        Email = "email-address@some-server.com",
        OTM = "D",
        `OTM Reason` = "otm-entered-by",
        Segment = "channel",
        `Sub Segments` = "sub-channel",
        `Parent Segment` = "globalChannel",
        `Account Type` = "",
        `Account Status` = "",
        `Webshop Registered` = "",
        `Loyalty Management Opt In` = "",
        `Number of meal served per day` = "150",
        `Avg. Price per Meal` = "12345.00",
        `Week/Year Open` = "4",
        `Food Spend Month` = "",
        `Convenience Level` = "cooking-convenience-level",
        Latitude = "latitude",
        Longitude = "longitude",
        `Customer Hierarchy Level 3 Desc` = "customerHierarchyLevel3",
        `Customer Hierarchy Level 4 Desc` = "customerHierarchyLevel4",
        `Customer Hierarchy Level 5 Desc` = "customerHierarchyLevel5",
        `Customer Hierarchy Level 7 Desc` = "customerHierarchyLevel7",
        MixedorUFS = "",
        SalesGroupKey = "",
        SalesOfficeKey = "",
        ECCIndustryKey = "",
        SalesDistrict = "",
        CustomerGroup = "",
        `Language Key` = "",
        `SAP Customer ID` = "",
        `Record Type` = "",
        `Indirect Account` = "",
        SourceName = "source-name",
        Website = "www.google.com",
        `Key Number` = "",
        `Has Webshop Account` = "",
        `Day Week Open` = "",
        `Do you have a terrace / outside seating?` = "",
        `Do you offer takeaways?` = "",
        `Do you offer a home delivery service?` = "",
        `How many seats do you have?` = "",
        `Kitchen Type` = "",
        `Number of Beds (Range) - Site` = "",
        `Number of Rooms (Range) - Site` = "",
        `Number of Students/Pupils (Range) - Site` = "",
        `Number of Employees (Range) - Group` = "",
        `Food Served Onsite` = "",
        `Conference Outlet On Site` = "",
        `Twitter URL` = "",
        `Facebook URL` = "",
        `Instagram URL` = "",
        `Number of child sites` = "",
        `Last login to website` = "",
        `Last newsletter opened` = "",
        `Subcribed to UFS newsletter` = "",
        `Subcribed to Ice Cream newsletter` = "",
        `Last 3 online events` = "",
        `Facebook campaigns clicked` = "",
        `LiveChat activities` = "",
        `Loyalty Rewards Balance Points` = "",
        `Loyalty Rewards Balance updated Date` = "",
        `Account Sub Type` = "",
        `Visitors per year` = "",
        `Unilever Now Classification` = "",
        `Trading Status` = "",
        CTPS = "",
        `Caterlyst Opportunity` = "",
        `Rep Assessed Opportunity` = "",
        `OTM OOH Calculated` = "",
        `OTM UFS Calculated` = "",
        `E-OTM` = "",
        `Relative lead score` = "",
        `Permitted to order` = "",
        `OFS value` = "",
        `RtM IC flag` = ""
      )
      result shouldBe expectedDdlOperator
    }
  }

}
