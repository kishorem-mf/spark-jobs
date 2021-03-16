package com.unilever.ohub.spark.export.ddl

import com.unilever.ohub.spark.domain.entity.WholesalerAssignment
import com.unilever.ohub.spark.export.{Converter, TypeConversionFunctions}
import com.unilever.ohub.spark.export.ddl.model.DdlWholesalerAssignment

object WholesalerAssignmentDdlConverter extends Converter[WholesalerAssignment, DdlWholesalerAssignment]
  with TypeConversionFunctions {
  override def convert(implicit ws: WholesalerAssignment, explain: Boolean = false): DdlWholesalerAssignment = {
    DdlWholesalerAssignment(
      `AFH WS Assignment Golden ID` = getValue("ohubId"),
      `WS Assignment ID` = getValue("sourceEntityId"),
      `AFH Customer Golden ID` = getValue("operatorOhubId"),
      `Customer SAP concat ID` = getValue("operatorConcatId"),
      `AFH Wholesaler Golden ID` = getValue("operatorOhubId"),
      `Wholesaler SAP concat ID` = getValue("operatorConcatId"),
      `Primary Foods` = getValue("isPrimaryFoodsWholesaler"),
      `Primary Ice Cream` = getValue("isPrimaryIceCreamWholesaler"),
      `Primary Foods CRM` = getValue("isPrimaryFoodsWholesalerCrm"),
      `Primary Ice Cream CRM` = getValue("isPrimaryIceCreamWholesalerCrm"),
      `WS Customer Primary Code` = getValue("sourceEntityId"),
      `WS Customer Code 2` = getValue("wholesalerCustomerCode2"),
      `WS Customer Code 3` = getValue("wholesalerCustomerCode3"),
      `SSD Status` = getValue("hasPermittedToShareSsd"),
      `External Source` = getValue("isProvidedByCrm"),
      `Wholesaler Assignment Code` = getValue("crmId"),
      `Delete Wholesaler Assignment` = getValue("isActive")
    )
  }
}
