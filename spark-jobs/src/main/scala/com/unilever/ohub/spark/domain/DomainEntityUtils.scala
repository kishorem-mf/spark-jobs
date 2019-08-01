package com.unilever.ohub.spark.domain

import org.reflections.Reflections

import scala.reflect.runtime.universe._

object DomainEntityUtils {
  /**
    * Function uses reflection to look for all [[com.unilever.ohub.spark.domain.DomainEntity]] objects
    * (under [[com.unilever.ohub.spark.domain.DomainEntity]]'s package) and fetches the
    * corresponding [[com.unilever.ohub.spark.domain.DomainEntityCompanion]]s.
    *
    * @return
    */
  lazy val domainCompanionObjects: Array[DomainEntityCompanion] = {
    val reflections = new Reflections(classOf[DomainEntity].getPackage.getName)
    val types = reflections.getSubTypesOf[DomainEntity](classOf[DomainEntity])

    val mirror = runtimeMirror(getClass.getClassLoader)

    types.toArray
      .map(_.asInstanceOf[java.lang.Class[DomainEntity]])
      .map((domainEntityClass) ⇒ {
        val module = mirror.staticModule(domainEntityClass.getName)
        mirror.reflectModule(module).instance.asInstanceOf[DomainEntityCompanion]
      })
  }
}
