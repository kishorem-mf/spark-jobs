package com.unilever.ohub.spark.export.ddl

import com.unilever.ohub.spark.domain.entity.Asset
import com.unilever.ohub.spark.export.{Converter, TypeConversionFunctions}
import com.unilever.ohub.spark.export.ddl.model.DdlAssets

object AssetDdlConverter extends Converter[Asset, DdlAssets] with TypeConversionFunctions {

  override def convert(implicit asset: Asset, explain: Boolean = false): DdlAssets = {
    DdlAssets(
      ID = getValue("crmId"),
      `Asset Name` = getValue("name"),
      `Asset Record Type` = getValue("type"),
      `Cabinet Branding` = getValue("brandName"),
      `Cabinet Code` = getValue("sourceEntityId"),
      `Description` = getValue("description"),
      `External ID` = getValue("concatId"),
      `Measures (L x W x H)` = getValue("dimensions"),
      `Number of Times Repaired` = getValue("numberOfTimesRepaired"),
      `Power Consumption` = getValue("powerConsumption"),
      `Quantity of Baskets (IE/GW)` = getValue("numberOfCabinetBaskets"),
      `Quantity of Facings` = getValue("numberOfCabinetFacings"),
      `Serial Number` = getValue("serialNumber"),
      `Status (OOH)` = getValue("oohClassification"),
      Status = getValue("lifecyclePhase"),
      `Useful Capacity (l)` = getValue("capacityInLiters"),
      `UDL Timestamp` = getValue("dateCreatedInUdl"),
      `FOL or Sold` = getValue("leasedOrSold"),
      `Task Relation` = getValue("crmTaskId")
    )
  }
}
