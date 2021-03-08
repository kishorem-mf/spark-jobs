package com.unilever.ohub.spark.export.ddl

import com.unilever.ohub.spark.domain.entity.OperatorGolden
import com.unilever.ohub.spark.export._
import com.unilever.ohub.spark.export.ddl.model.DdlOperator

object OperatorDdlConverter extends Converter[OperatorGolden, DdlOperator] with TypeConversionFunctions {

  // scalastyle:off method.length
  override def convert(implicit op: OperatorGolden, explain: Boolean = false): DdlOperator = {
    DdlOperator(
      `AFH Customer Golden ID` = getValue("ohubId"),
      `CRM Account ID` = Option.empty,
      `Customer SAP concat ID` = getValue("sourceEntityId"),
      Channel = Option.empty,
      Division = Option.empty,
      SalesOrgID = Option.empty,
      ParentSourceCustomerCode = Option.empty,
      `Closing Time Working Day` = Option.empty,
      `Opening Time Working Day` = Option.empty,
      `Preferred Visit Days` = Option.empty,
      `Preferred Visit Start Time` = Option.empty,
      `Preferred Visit End Time` = Option.empty,
      `Preferred Delivery Day/s` = Option.empty,
      `Preferred Visit week of Month` = Option.empty,
      `Customer Name` = getValue("name"),
      `Customer Name 2` = Option.empty,
      `Address Line 1` = getValue("street"),
      City = getValue("city"),
      State = getValue("state"),
      `Postal Code` = getValue("zipCode"),
      Country = getValue("countryName"),
      Region = getValue("region"),
      `Phone Number 1` = getValue("phoneNumber"),
      Fax = getValue("faxNumber"),
      Email = getValue("emailAddress"),
      OTM = getValue("otm"),
      `OTM Reason` = getValue("otmEnteredBy"),
      Segment = getValue("channel"),
      `Sub Segments` = getValue("subChannel"),
      `Parent Segment` = getValue("globalChannel"),
      `Account Type` = Option.empty,
      `Account Status` = Option.empty,
      `Webshop Registered` = Option.empty,
      `Loyalty Management Opt In` = Option.empty,
      `Number of meal served per day` = getValue("totalDishes"),
      `Avg. Price per Meal` = getValue("averagePrice"),
      `Week/Year Open` = getValue("daysOpen"),
      `Food Spend Month` = Option.empty,
      `Convenience Level` = getValue("cookingConvenienceLevel"),
      Latitude = Option.empty,
      Longitude = Option.empty,
      `Customer Hierarchy Level 3 Desc` = Option.empty,
      `Customer Hierarchy Level 4 Desc` = Option.empty,
      `Customer Hierarchy Level 5 Desc` = Option.empty,
      `Customer Hierarchy Level 7 Desc` = Option.empty,
      MixedorUFS = Option.empty,
      SalesGroupKey = Option.empty,
      SalesOfficeKey = Option.empty,
      ECCIndustryKey = Option.empty,
      SalesDistrict = Option.empty,
      CustomerGroup = Option.empty,
      `Language Key` = Option.empty,
      `SAP Customer ID` = Option.empty,
      `Record Type` = Option.empty,
      `Indirect Account` = Option.empty,
      SourceName = getValue("sourceName"),
      Website = getValue("website"),
      `Key Number` = Option.empty,
      `Has Webshop Account` = Option.empty,
      `Day Week Open` = Option.empty,
      `Do you have a terrace / outside seating?` = Option.empty,
      `Do you offer takeaways?` = Option.empty,
      `Do you offer a home delivery service?` = Option.empty,
      `How many seats do you have?` = Option.empty,
      `Kitchen Type` = Option.empty,
      `Number of Beds (Range) - Site` = Option.empty,
      `Number of Rooms (Range) - Site` = Option.empty,
      `Number of Students/Pupils (Range) - Site` = Option.empty,
      `Number of Employees (Range) - Group` = Option.empty,
      `Food Served Onsite` = Option.empty,
      `Conference Outlet On Site` = Option.empty,
      `Twitter URL` = Option.empty,
      `Facebook URL` = Option.empty,
      `Instagram URL` = Option.empty,
      `Number of child sites` = Option.empty,
      `Last login to website` = Option.empty,
      `Last newsletter opened` = Option.empty,
      `Subcribed to UFS newsletter` = Option.empty,
      `Subcribed to Ice Cream newsletter` = Option.empty,
      `Last 3 online events` = Option.empty,
      `Facebook campaigns clicked` = Option.empty,
      `LiveChat activities` = Option.empty,
      `Loyalty Rewards Balance Points` = Option.empty,
      `Loyalty Rewards Balance updated Date` = Option.empty,
      `Account Sub Type` = Option.empty,
      `Visitors per year` = Option.empty,
      `Unilever Now Classification` = Option.empty,
      `Trading Status` = Option.empty,
      CTPS = Option.empty,
      `Caterlyst Opportunity` = Option.empty,
      `Rep Assessed Opportunity` = Option.empty,
      `OTM OOH Calculated` = Option.empty,
      `OTM UFS Calculated` = Option.empty,
      `E-OTM` = Option.empty,
      `Relative lead score` = Option.empty,
      `Permitted to order` = Option.empty,
      `OFS value` = Option.empty,
      `RtM IC flag` = Option.empty
    )
  }
}
