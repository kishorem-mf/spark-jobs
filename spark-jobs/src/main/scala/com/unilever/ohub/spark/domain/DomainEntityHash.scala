package com.unilever.ohub.spark.domain

case class DomainEntityHash(concatId: String, hasChanged: Option[Boolean], md5Hash: Option[String])
