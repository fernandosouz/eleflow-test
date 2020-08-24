package com.eleflow.br.services

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, DecimalType, IntegerType}
import com.eleflow.br.schemas.Schemas
import com.eleflow.br.setup.ApplicationContext.SPARK

object StandardizeImplicits {

  def normalizeDecimalUDF = udf(normalizeDecimal(_:String): Option[BigDecimal])
  def normalizeIntegerUDF = udf(normalizeInteger(_:String): Option[Integer])
  SPARK.udf.register("normalizeDecimalUDF", normalizeDecimal(_:String): Option[BigDecimal])
  SPARK.udf.register("normalizeIntegerUDF", normalizeInteger(_:String): Option[Integer])

  def isTrace(value:String): Boolean = "-".equals(value)
  def replaceComma(value:String): String = value.replace(",", ".")

  def normalizeDecimal(value: String): Option[BigDecimal] =
    if (isTrace(value)) None
    else Some(BigDecimal(replaceComma(value)))

  def normalizeInteger(value: String): Option[Integer] =
    if (isTrace(value)) None
    else Some(replaceComma(value).toInt)


  class StandardizeImplicits(dataFrame: DataFrame) {
    def standardize: DataFrame =
      Schemas.weeklyCities2019.foldLeft(dataFrame)((accDf, field) => {
        field.dataType match {
          case _: DateType => accDf.withColumn(field.name, to_date(col(field.name), "dd/MM/yyyy"))
          case _: DecimalType => accDf.withColumn(field.name, normalizeDecimalUDF(col(field.name)).cast(DecimalType(9,3)))
          case _: IntegerType => accDf.withColumn(field.name, normalizeIntegerUDF(col(field.name)).cast(IntegerType))
          case _ => accDf
        }
      })
  }

  implicit def standardizeImplicits(dataFrame: DataFrame): StandardizeImplicits = new StandardizeImplicits(dataFrame)
}
