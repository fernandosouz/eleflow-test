package com.eleflow.br.schemas

import org.apache.spark.sql.types._

object Schemas {

  val weeklyCities2019: StructType = StructType(
    Array(
        StructField("DATA INICIAL", DateType, nullable = true),
        StructField("DATA FINAL", DateType, nullable = true),
        StructField("REGIÃO", StringType, nullable = true),
        StructField("ESTADO", StringType, nullable = true),
        StructField("MUNICÍPIO", StringType, nullable = true),
        StructField("PRODUTO", StringType, nullable = true),
        StructField("NÚMERO DE POSTOS PESQUISADOS", IntegerType, nullable = true),
        StructField("UNIDADE DE MEDIDA", StringType, nullable = true),
        StructField("PREÇO MÉDIO REVENDA", DecimalType(9, 3), nullable = true),
        StructField("DESVIO PADRÃO REVENDA", DecimalType(9, 3), nullable = true),
        StructField("PREÇO MÍNIMO REVENDA", DecimalType(9, 3), nullable = true),
        StructField("PREÇO MÁXIMO REVENDA", DecimalType(9, 3), nullable = true),
        StructField("MARGEM MÉDIA REVENDA", DecimalType(9, 3), nullable = true),
        StructField("COEF DE VARIAÇÃO REVENDA", DecimalType(9, 3), nullable = true),
        StructField("PREÇO MÉDIO DISTRIBUIÇÃO", DecimalType(9, 3), nullable = true),
        StructField("DESVIO PADRÃO DISTRIBUIÇÃO", DecimalType(9, 3), nullable = true),
        StructField("PREÇO MÍNIMO DISTRIBUIÇÃO", DecimalType(9, 3), nullable = true),
        StructField("PREÇO MÁXIMO DISTRIBUIÇÃO", DecimalType(9, 3), nullable = true),
        StructField("COEF DE VARIAÇÃO DISTRIBUIÇÃO", DecimalType(9, 3), nullable = true)
    )
  )
}
