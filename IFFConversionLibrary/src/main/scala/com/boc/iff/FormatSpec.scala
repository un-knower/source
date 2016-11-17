package com.boc.iff

import java.lang.reflect.Method
import java.text.Format

import scala.beans.BeanProperty
import scala.collection.mutable

/**
  * Created by cvinc on 2016/6/8.
  */
object FormatSpec {

  def main(args: Array[String]) {
    val f = new FormatSpec
    f.className="java.text.SimpleDateFormat"
    f.pattern = "yyyy/MM/dd"
    println(f.getFormatObj.format(new java.util.Date()))
  }
}

class FormatSpec extends Serializable {

  @BeanProperty
  var className: String = null
  @BeanProperty
  var pattern: String = null

  private var formatObj: Format = null

  def getFormatObj: Format = {
    if(formatObj != null) return formatObj
    var formatClass: Class[_] = null
    formatClass = Class.forName(className)
    val constructor = formatClass.getConstructor(Array[Class[_]](this.pattern.getClass):_*)
    val obj = constructor.newInstance(Array[Object](this.pattern):_*)
    formatObj = obj match {
      case obj: Format =>
        obj.asInstanceOf[Format]
      case _ =>
        throw new InstantiationException("Class " + className + " is not a format class.")
    }
    formatObj
  }

  override def toString: String = {
    val map = mutable.LinkedHashMap("className"->className, "pattern"->pattern, "formatObj" -> formatObj)
    val sb = new StringBuilder()
    sb ++= "FormatSpec ["
    var first = true
    for((key, value)<-map) {
      if (value != null) {
        if (first) first = false
        else sb ++= ", "
        sb ++= key
        sb ++= "="
        sb ++= value.toString
      }
    }
    sb ++= "]"
    sb.toString
  }

  def toXMLString: String = {
    val sb = new StringBuilder()
    sb ++= ("<bean class=\"" + this.getClass.getName + "\">")
    sb ++= ("<property name=\"className\" value=\"" + className + "\"/>")
    sb ++= ("<property name=\"pattern\" value=\"" + pattern + "\"/>")
    sb ++= ("</bean>")
    sb.toString
  }
}
