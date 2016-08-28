package com.boc.iff.model

import com.boc.iff.FormatSpec

import scala.beans.{BeanProperty, BooleanBeanProperty}
import scala.collection.mutable
import scala.math.Ordering

/**
  * Created by cvinc on 2016/6/8.
  */

class IFFField extends Serializable {

  @BeanProperty
  var name: String = _

  @BeanProperty
  var startPos: Int = _

  @BeanProperty
  var endPos: Int = _

  @BeanProperty
  var `type`: String = _

  @BeanProperty
  var typeInfo: IFFFieldType = null

  @BooleanBeanProperty
  var filler: Boolean = false

  @BooleanBeanProperty
  var constant: Boolean = false

  @BooleanBeanProperty
  var required: Boolean = false

  @BeanProperty
  var nullValue: String = ""

  @BeanProperty
  var defaultValue: String = ""

  @BeanProperty
  var formatSpec: FormatSpec = _

  def toXMLString: String = {
    val sb = new StringBuilder
    sb ++= ("<bean class=\"" + this.getClass.getName + "\">")
    sb ++= ("<property name=\"name\" value=\"" + name + "\"/>")
    sb ++= ("<property name=\"startPos\" value=\"" + startPos + "\"/>")
    sb ++= ("<property name=\"endPos\" value=\"" + endPos + "\"/>")
    sb ++= ("<property name=\"type\" value=\"" + `type` + "\"/>")
    sb ++= ("<property name=\"filler\" value=\"" + (if (filler) "true" else "false") + "\"/>")
    sb ++= ("<property name=\"required\" value=\"" + (if (required) "true" else "false") + "\"/>")
    sb ++= ("<property name=\"constant\" value=\"" + (if (constant) "true" else "false") + "\"/>")
    sb ++= ("<property name=\"nullValue\" value=\"" + nullValue + "\"/>")
    sb ++= ("<property name=\"defaultValue\" value=\"" + defaultValue + "\"/>")
    if (formatSpec != null) {
      sb ++= "<property name=\"formatSpec\">"
      sb ++= formatSpec.toXMLString
      sb ++= "</property>"
    }
    sb ++= "</bean>"
    sb.toString
  }

  override def toString: String = {
    val map = mutable.LinkedHashMap(
      "name" -> name,
      "startPos" -> startPos,
      "endPos" -> endPos,
      "type" -> `type`,
      "filler" -> filler,
      "constant" -> constant,
      "nullValue" -> nullValue,
      "defaultValue" -> defaultValue,
      "formatSpec" -> formatSpec
    )
    val sb = new StringBuilder()
    sb ++= "IFFField ["
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
}

trait IFFFieldOrdring extends Ordering[IFFField]{
  outer =>

  def compare(x: IFFField, y: IFFField) = java.lang.Integer.compare(x.startPos, y.startPos)
  override def lteq(x: IFFField, y: IFFField): Boolean = x <= y
  override def gteq(x: IFFField, y: IFFField): Boolean = x >= y
  override def lt(x: IFFField, y: IFFField): Boolean = x < y
  override def gt(x: IFFField, y: IFFField): Boolean = x > y
  override def equiv(x: IFFField, y: IFFField): Boolean = x == y

  override def reverse: Ordering[IFFField] = new IFFFieldOrdring {
    override def reverse = outer
    override def compare(x: IFFField, y: IFFField) = outer.compare(y, x)
    override def lteq(x: IFFField, y: IFFField): Boolean = outer.lteq(y, x)
    override def gteq(x: IFFField, y: IFFField): Boolean = outer.gteq(y, x)
    override def lt(x: IFFField, y: IFFField): Boolean = outer.lt(y, x)
    override def gt(x: IFFField, y: IFFField): Boolean = outer.gt(y, x)
  }
}

