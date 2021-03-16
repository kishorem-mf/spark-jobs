package com.unilever.ohub.spark.export.ddl

import com.unilever.ohub.spark.domain.entity.AssetMovement
import com.unilever.ohub.spark.export.{Converter, TypeConversionFunctions}
import com.unilever.ohub.spark.export.ddl.model.DdlAssetMovements

object AssetMovementDdlConverter extends Converter[AssetMovement, DdlAssetMovements] with TypeConversionFunctions {

  override def convert(implicit asset: AssetMovement, explain: Boolean = false): DdlAssetMovements = {
    DdlAssetMovements(
      ID = getValue("crmId"),
      Account = getValue("operatorOhubId"),
      `Assembly Date` = getValue("assemblyDate"),
      Asset = getValue("assetConcatId"),
      CabinetCode = getValue("sourceEntityId"),
      `Created By` = getValue("createdBy"),
      Currency = getValue("currency"),
      `Delivery Date` = getValue("deliveryDate"),
      `Last Modified By` = getValue("lastModifiedBy"),
      Name = getValue("name"),
      Notes = getValue("comment"),
      Owner = getValue("owner"),
      Quantity = getValue("quantityOfUnits"),
      `Record Type` = getValue("type"),
      `Return Date` = getValue("returnDate"),
      Status = getValue("assetStatus")
    )
  }

}
