package com.boc.iff

import java.io.FileInputStream
import java.util
import java.util.Properties

import com.googlecode.aviator.AviatorEvaluator
import com.googlecode.aviator.Expression
import org.mvel2.compiler.{CompiledExpression, ExpressionCompiler}
import org.mvel2.MVEL

/**
  * Created by scutlxj on 2016/12/20.
  */
abstract trait ExpressionProcessor {
  //var logger:ECCLogger = null

  def initExpression(expression: String):Unit

  def getValue(params:java.util.Map[String,Any]):Any

}

trait AviatorExpressionProcessor extends ExpressionProcessor{

  var compiledExp:Expression = null

  var exp:String = null

  def initExpression(expression: String):Unit={
    exp = expression
  }

  override def getValue(params: util.Map[String,Any]): Any = {
    /*if(logger==null) {
      logger = new ECCLogger()
      val prop = new Properties()
      prop.load(new FileInputStream("/app/birdie/bochk/IFFConversion/config/config.properties"))
      logger.configure(prop)
    }*/
    if(compiledExp==null){
      //logger.info("SN","************compile "+expression)
      compiledExp = AviatorEvaluator.compile(exp)
    }
    compiledExp.execute(params.asInstanceOf[util.Map[String,Object]])
  }
}

trait MvelExpressionProcessor extends ExpressionProcessor{
  var compiledExp:CompiledExpression = null

  def initExpression(expression: String):Unit={
    compiledExp = new ExpressionCompiler(expression).compile()
  }
  override def getValue(params: util.Map[String,Any]): Any = {
    MVEL.executeExpression(compiledExp, params)
  }
}
