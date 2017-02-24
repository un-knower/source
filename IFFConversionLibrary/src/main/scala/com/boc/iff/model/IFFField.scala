package com.boc.iff.model

import java.util

import com.boc.iff.{AviatorExpressionProcessor, FormatSpec}
import org.apache.commons.lang.StringUtils
import org.mvel2.compiler.CompiledExpression

import scala.beans.{BeanProperty, BooleanBeanProperty}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.math.Ordering

/**
  * Created by cvinc on 2016/6/8.
  */

class IFFField extends Serializable with AviatorExpressionProcessor {

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
  var filter: Boolean = false

  @BooleanBeanProperty
  var constant: Boolean = false

  @BooleanBeanProperty
  var required: Boolean = false

  @BeanProperty
  var nullValue: String = ""

  @BeanProperty
  var defaultValue: String = ""

  @BeanProperty
  var expression: String = ""

  @BeanProperty
  var fieldExpression: String = ""

  @BeanProperty
  var formatSpec: FormatSpec = _

  @BeanProperty
  var validators: java.util.List[String] = new util.ArrayList[String]

  @BeanProperty
  var primaryKey: Boolean = false

  @BeanProperty
  var virtual: Boolean = false


  def initExpression:Unit = if(StringUtils.isNotEmpty(expression))initExpression(expression)

  def getExpressionValue(params:java.util.Map[String,Any]):Any={
    getValue(params)
  }

  def toXMLString: String = {
    val sb = new StringBuilder
    sb ++= ("<bean class=\"" + this.getClass.getName + "\">")
    sb ++= ("<property name=\"name\" value=\"" + name + "\"/>")
    sb ++= ("<property name=\"startPos\" value=\"" + startPos + "\"/>")
    sb ++= ("<property name=\"endPos\" value=\"" + endPos + "\"/>")
    sb ++= ("<property name=\"type\" value=\"" + `type` + "\"/>")
    sb ++= ("<property name=\"filter\" value=\"" + (if (filter) "true" else "false") + "\"/>")
    sb ++= ("<property name=\"required\" value=\"" + (if (required) "true" else "false") + "\"/>")
    sb ++= ("<property name=\"constant\" value=\"" + (if (constant) "true" else "false") + "\"/>")
    sb ++= ("<property name=\"nullValue\" value=\"" + nullValue + "\"/>")
    sb ++= ("<property name=\"expression\" value=\"" + expression + "\"/>")
    sb ++= ("<property name=\"defaultValue\" value=\"" + defaultValue + "\"/>")
    if (formatSpec != null) {
      sb ++= "<property name=\"formatSpec\">"
      sb ++= formatSpec.toXMLString
      sb ++= "</property>"
    }
    sb ++= ("<property name=\"primaryKey\" value=\"" + (if (primaryKey) "true" else "false") + "\"/>")
    sb ++= ("<property name=\"virtual\" value=\"" + (if (virtual) "true" else "false") + "\"/>")
    sb ++= ("<property name=\"fieldExpression\" value=\"" + fieldExpression+ "\"/>")
    sb ++= "</bean>"
    sb.toString
  }

  override def toString: String = {
    val map = mutable.LinkedHashMap(
      "name" -> name,
      "startPos" -> startPos,
      "endPos" -> endPos,
      "type" -> `type`,
      "filter" -> filter,
      "constant" -> constant,
      "nullValue" -> nullValue,
      "defaultValue" -> defaultValue,
      "expression" -> expression,
      "formatSpec" -> formatSpec,
      "primaryKey" -> primaryKey,
      "virtual" -> virtual
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

