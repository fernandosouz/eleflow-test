package com.eleflow.br.models

case class Configuration(
                          inputPath: String,
                          show: Boolean,
                          outputPath: Option[String]
                        )