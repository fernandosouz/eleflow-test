package com.eleflow.br.services

import com.eleflow.br.models.WCities
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

object QueriesImplicits {

  class QueriesImplicits(dataFrame: DataFrame) {

    val avgLabel = "MEDIA DE"
    val monthAndYearFormat = "M/yyyy"
    val sourceDateFormat = "dd/MM/yyyy"
    val monthAndYearColumn = "MES E ANO"
    val maxColumn = "MAX"
    val minColumn = "MIN"

    def avgProductsByCityAndMonth: DataFrame = {

      val (groupByColumns, calcAvgColumns, orderByColumns) = getAvgProductsByCityAndMonthProperties

      dataFrame
        .withColumn(monthAndYearColumn, date_format(unix_timestamp(col(WCities.DATA_FINAL), sourceDateFormat).cast(TimestampType), monthAndYearFormat))
        .groupBy(groupByColumns.map(col): _*)
        .agg(
          avg(col(calcAvgColumns.head)).as(s"$avgLabel ${calcAvgColumns.head}"),
          calcAvgColumns.tail.map(fieldName => avg(col(fieldName)).as(s"$avgLabel $fieldName")):_*)
        .orderBy(orderByColumns.map(col): _*)
    }

    def avgProductsByStateAndRegion: DataFrame = {

      val (groupByColumns, calcAvgColumns, orderByColumns) = getAvgProductsByStateAndRegionProperties

      dataFrame
        .groupBy(groupByColumns.map(col): _*)
        .agg(
          avg(col(calcAvgColumns.head)).as(s"$avgLabel ${calcAvgColumns.head}"),
          calcAvgColumns.tail.map(fieldName => avg(col(fieldName)).as(s"$avgLabel $fieldName")):_*)
        .orderBy(orderByColumns.map(col): _*)
    }

    def minAndMaxVarianceAndVariation: DataFrame = {

      val avgMaxColumn = "AVG MAX"
      val avgMinColumn = "AVG MIN"
      val maxColumnVariance = "VARIANCIA MAX"
      val minColumnVariance = "VARIANCIA MIN"
      val absoluteVariationColumn = "VARIAÇÃO ABSOLUTA"

      dataFrame
        .withColumn(monthAndYearColumn, date_format(unix_timestamp(col(WCities.DATA_FINAL), sourceDateFormat).cast(TimestampType), monthAndYearFormat))
        .groupBy(col(monthAndYearColumn), col(WCities.MUNICIPIO))
        .agg(
          avg(col(WCities.PRECO_MAX_REVENDA)).as(avgMaxColumn),
          avg(col(WCities.PRECO_MIN_REVENDA)).as(avgMinColumn),
          max(col(WCities.PRECO_MAX_REVENDA)).as(maxColumn),
          min(col(WCities.PRECO_MIN_REVENDA)).as(minColumn)
        )
        .withColumn(maxColumnVariance, col(maxColumn) - col(avgMaxColumn))
        .withColumn(minColumnVariance, col(minColumn) - col(avgMinColumn))
        .withColumn(absoluteVariationColumn, col(maxColumn) - col(minColumn))
        .select(monthAndYearColumn, WCities.MUNICIPIO,  maxColumnVariance,minColumnVariance, absoluteVariationColumn)
        .orderBy(WCities.MUNICIPIO, monthAndYearColumn)
    }

    def citiesWithMoreDiffBetweenProducts: DataFrame = {

      val diffColumn = "DIFERENÇA"


      def calcDiff = col(maxColumn) - col(minColumn)

      dataFrame
        .groupBy(WCities.MUNICIPIO)
        .agg(
          max(col(WCities.PRECO_MAX_REVENDA)).as(maxColumn),
          min(col(WCities.PRECO_MIN_REVENDA)).as(minColumn))
        .withColumn(diffColumn, calcDiff)
        .orderBy(col(diffColumn).desc)
        .select(WCities.MUNICIPIO)
        .limit(5)
    }

    def getAvgProductsByStateAndRegionProperties = {
      val groupByColumns = Seq(WCities.REGIAO, WCities.ESTADO, WCities.PRODUTO)
      val calcAvgColumns = Seq(WCities.PRECO_MEDIO_DISTRIBUICAO)
      val orderByColumns = Seq(WCities.REGIAO, WCities.ESTADO)

      (groupByColumns, calcAvgColumns, orderByColumns)
    }

    def getAvgProductsByCityAndMonthProperties = {
      val groupByColumns = Seq(monthAndYearColumn, WCities.MUNICIPIO, WCities.PRODUTO)

      val calcAvgColumns = Seq(
        WCities.PRECO_MAX_REVENDA,
        WCities.PRECO_MAX_DISTRIBUICAO,
        WCities.PRECO_MEDIO_DISTRIBUICAO,
        WCities.PRECO_MEDIO_REVENDA,
        WCities.PRECO_MIN_REVENDA)

      val orderByColumns =  Seq(monthAndYearColumn, WCities.MUNICIPIO, WCities.PRODUTO)

      (groupByColumns,calcAvgColumns,orderByColumns)
    }

  }

  implicit def queriesImplicits(dataFrame: DataFrame): QueriesImplicits = new QueriesImplicits(dataFrame)
}
