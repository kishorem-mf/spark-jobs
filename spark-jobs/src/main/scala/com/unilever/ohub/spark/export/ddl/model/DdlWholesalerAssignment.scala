package com.unilever.ohub.spark.export.ddl.model

import com.unilever.ohub.spark.export.DDLOutboundEntity

case class DdlWholesalerAssignment(
                                    `AFH WS Assignment Golden ID`: String,
                                    `WS Assignment ID`: String,
                                    `AFH Customer Golden ID`: String,
                                    `Customer SAP concat ID`: String,
                                    `AFH Wholesaler Golden ID`: String,
                                    `Wholesaler SAP concat ID`: String,
                                    `Primary Foods`: String,
                                    `Primary Ice Cream`: String,
                                    `Primary Foods CRM`: String,
                                    `Primary Ice Cream CRM`: String,
                                    `WS Customer Primary Code`: String,
                                    `WS Customer Code 2`: String,
                                    `WS Customer Code 3`: String,
                                    `SSD Status`: String,
                                    `External Source`: String,
                                    `Wholesaler Assignment Code`: String,
                                    `Delete Wholesaler Assignment`: String
                                  ) extends DDLOutboundEntity
