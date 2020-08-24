package com.eleflow.br.models

case class Configuration(
                          inputPath: String,
                          format: String,
                          show: Boolean,
                          outputPath: Option[String]
                        )