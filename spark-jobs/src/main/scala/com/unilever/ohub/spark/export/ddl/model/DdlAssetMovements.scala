package com.unilever.ohub.spark.export.ddl.model

import com.unilever.ohub.spark.export.DDLOutboundEntity

case class DdlAssetMovements(
                              ID: String,
                              Account: String,
                              `Assembly Date`: String,
                              Asset: String,
                              CabinetCode: String,
                              `Created By`: String,
                              Currency: String,
                              `Delivery Date`: String,
                              `Last Modified By`: String,
                              Name: String,
                              Notes: String,
                              Owner: String,
                              Quantity: String,
                              `Record Type`: String,
                              `Return Date`: String,
                              Status: String
                            ) extends DDLOutboundEntity
