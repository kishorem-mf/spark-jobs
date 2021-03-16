package com.unilever.ohub.spark.export.ddl.model

import com.unilever.ohub.spark.export.DDLOutboundEntity

case class DdlAssets (
                       ID: String,
                       `Asset Name`: String,
                       `Asset Record Type`: String,
                       `Cabinet Branding`: String,
                       `Cabinet Code`: String,
                       `Description`: String,
                       `External ID`: String,
                       `Measures (L x W x H)`: String,
                       `Number of Times Repaired`: String,
                       `Power Consumption`: String,
                       `Quantity of Baskets (IE/GW)`: String,
                       `Quantity of Facings`: String,
                       `Serial Number`: String,
                       `Status (OOH)`: String,
                       Status: String,
                       `Useful Capacity (l)`: String,
                       `UDL Timestamp`: String,
                       `FOL or Sold`: String,
                       `Task Relation`: String
                     ) extends DDLOutboundEntity
