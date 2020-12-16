package com.unilever.ohub.spark.insights

object InsightConstants {

  val SEMICOLON = ";"
  val ESCAPE_BACKSLASH = "\\"
  val DATA_COMPLETENESS_FILENAME = "DataCompletenessInsights.csv"
  val RECORD_LINKING_FILENAME = "EntityLinkingInsights.csv"

  val DATA_COMPLETENESS_FILE_PATTERN = "(^UFS_).*(\\.(?i)csv)"
  val BASE_FILE_NAME_INDEX = 38

  val AUDIT_TRAILS_QUERY = "select " +
    "FU.file_name," +
    "FU.status, " +
    "FU.timestamp::date, " +
    "CASE WHEN char_length(FU.user_name)-char_length(replace(FU.user_name,'-',''))=4 THEN spn.spnname ELSE FU.user_name END AS UserName " +
    "FROM inbound.AUDIT_TRAILS AS FU " +
    "right join (select file_name, max(version) as version from inbound.audit_trails group by file_name) as at2 " +
    " on FU.file_name = at2.file_name and FU.version = at2.version " +
    "LEFT JOIN inbound.adgroupusers AS ad " +
    "ON FU.user_name = ad.username " +
    "LEFT JOIN inbound.serviceprincipals as spn " +
    "ON  spn.spnid = FU.user_name " +
    "where (FU.status='COMPLETED' OR FU.status='FAILED' OR FU.status='EXECUTING')  " +
    "GROUP BY FU.file_name, FU.version, ad.country, spn.spnname"

  val CONTACTPERSON_FPO_FIELDS = Seq("FIRST_NAME", "LAST_NAME", "ZIP_CODE", "STREET", "CITY", "MOBILE_PHONE_NUMBER", "EMAIL_ADDRESS",
    "EM_OPT_IN_CONFIRMED", "EM_OPT_IN_CONFIRMED_DATE", "EM_OPT_IN", "EM_OPT_IN_DATE", "EM_OPT_OUT", "MOB_OPT_IN_CONFIRMED",
    "MOB_OPT_IN_CONFIRMED_DATE", "MOB_OPT_IN", "MOB_OPT_IN_DATE", "MOB_OPT_OUT")
  val OPERATOR_FPO_FIELDS = Seq("NAME", "ZIP_CODE", "STREET", "CITY", "CHANNEL")


  val FILE_UPLOAD_ERRORS_FILENAME = "FileUploadErrorsInsights.csv"
  val FILE_UPLOADS_QUERY = "select * from inbound.audit_trails as at1 " +
    "right join (select file_name, max(version) as version from inbound.audit_trails group by file_name) as at2" +
    "       on at1.file_name = at2.file_name and at1.version = at2.version " +
    "where at1.status = 'COMPLETED' or at1.status = 'FAILED'"
    //"and  DATE(timestamp) ='${execDate}'"
  val FILE_UPLOAD_ERROR_QUERY = "select * from inbound.errors"
  val BATCH_JOB_EXEC_QUERY = "select * from inbound.batch_job_execution_params"
  val BATCH_STEP_QUERY = "select * from inbound.batch_step_execution"
  val AD_GROUP_USERS_QUERY = "select * from inbound.adgroupusers"
  val SPN_QUERY = "select * from inbound.serviceprincipals"

  val DATA_COMPLETENESS_COLUMNS = Seq("FILE_NAME", "MODEL", "SOURCE", "TOTAL_ROW_COUNT", "DATA_FILLED_PERCENTAGE", "FPO_DATA_FILLED_PERCENTAGE",
    "STATUS", "CREATED_DATE", "USER_NAME")
  val FILE_UPLOAD_ERROR_COLUMNS = Seq("FILE_NAME", "TOTAL_RECORDS", "ERROR_TYPE_COUNT_PER_FILE", "ERROR_MESSAGE", "IS_WARNING",
    "STATUS", "TIMESTAMP", "REASON_FAILED", "LINKED_FILE", "USER_NAME", "SOURCE_NAME", "ENTITY_NAME", "COUNTRY")
}
