import com.eleflow.br.models.WCities
import com.eleflow.br.services.QueriesImplicits.queriesImplicits
import com.eleflow.br.setup.SparkProvider
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class QueriesTest extends FunSuite with BeforeAndAfterAll {


  var SPARK: SparkSession = SparkProvider.local()
  val spark: SparkSession = SPARK
  import spark.implicits._

  val mediaDe = "MÉDIA DE"
  val mounthAndYear = "MÊS E ANO"


  override def afterAll():Unit = {
    SPARK.stop()
  }

  test("Avarage Products By City And Month") {

    val resultDf = firstSourceDf.avgProductsByCityAndMonth

    val expectedResultDf = Seq(
      ("5/2020","CAMPINAS","GASOLINA",11.0,16.0,13.0,9.0,4.0),
      ("7/2020","SÃO PAULO","GASOLINA",11.0,16.0,13.0,9.0,4.0)
    ).toDF(
      mounthAndYear,
      WCities.MUNICIPIO,
      WCities.PRODUTO,
      s"$mediaDe ${WCities.PRECO_MAX_REVENDA}",
      s"$mediaDe ${WCities.PRECO_MAX_DISTRIBUICAO}",
      s"$mediaDe ${WCities.PRECO_MEDIO_DISTRIBUICAO}",
      s"$mediaDe ${WCities.PRECO_MEDIO_REVENDA}",
      s"$mediaDe ${WCities.PRECO_MIN_REVENDA}")

    val diff = expectedResultDf.except(resultDf)
    assert(diff.isEmpty)
  }

  test("Avarage Products By State And Region") {
    val resultDf = secondSourceDf.avgProductsByStateAndRegion

    val expectedResultDf = Seq(
      ("NORDESTE", "BAHIA","ETANOL HIDRATADO", 13.0),
      ("NORDESTE", "BAHIA","GASOLINA", 10.5),
      ("SUDESTE", "RIO DE JANEIRO","ETANOL HIDRATADO", 13.0),
      ("SUDESTE", "RIO DE JANEIRO","GASOLINA", 10.5),
      ("SUDESTE", "SÃO PAULO","ETANOL HIDRATADO", 13.0),
      ("SUDESTE", "SÃO PAULO","GASOLINA", 10.5)
    ).toDF(WCities.REGIAO,WCities.ESTADO,WCities.PRODUTO, s"$mediaDe ${WCities.PRECO_MEDIO_DISTRIBUICAO}")

    val diff = expectedResultDf.except(resultDf)

    assert(diff.isEmpty)
  }

  test("Min And Max Variance And Variation") {
    val resultDf = thirdSourceDf.minAndMaxVarianceAndVariation

    val expectedResultDf = Seq(
      ("5/2020","CAMPINAS",1.0,-1.0,-3.0),
      ("7/2020","SÃO PAULO",1.0,-1.0,-3.0)
    ).toDF(mounthAndYear, WCities.MUNICIPIO,"VARIANCIA MAX", "VARIANCIA MIN", "VARIAÇÃO ABSOLUTA")

    val diff = expectedResultDf.except(resultDf)

    assert(diff.isEmpty)
  }

  test("Cities With More Diff Between Products") {

    val resultDf = fourthSourceDf.citiesWithMoreDiffBetweenProducts

    val expectedResultDf = Seq(
      "MONTE ALEGRE DO SUL",
      "SÃO PAULO",
      "CAMPINAS",
      "PEDREIRA",
      "JAGUARIÚNA"
    ).toDF(WCities.MUNICIPIO)

    val diff = expectedResultDf.except(resultDf)
    assert(diff.isEmpty)
  }



  def firstSourceDf: DataFrame = {
    Seq(
      ("30/05/2020", "CAMPINAS", "GASOLINA", 10.000, 15.000, 12.000, 8.000, 3.000),
      ("30/05/2020", "CAMPINAS", "GASOLINA", 11.000, 16.000, 13.000, 9.000, 4.000),
      ("30/05/2020", "CAMPINAS", "GASOLINA", 12.000, 17.000, 14.000, 10.000, 5.000),
      ("30/07/2020", "SÃO PAULO", "GASOLINA", 10.000, 15.000, 12.000, 8.000, 3.000),
      ("30/07/2020", "SÃO PAULO", "GASOLINA", 11.000, 16.000, 13.000, 9.000, 4.000),
      ("30/07/2020", "SÃO PAULO", "GASOLINA", 12.000, 17.000, 14.000, 10.000, 5.000)
    ).toDF(WCities.DATA_FINAL, WCities.MUNICIPIO, WCities.PRODUTO, WCities.PRECO_MAX_REVENDA, WCities.PRECO_MAX_DISTRIBUICAO, WCities.PRECO_MEDIO_DISTRIBUICAO, WCities.PRECO_MEDIO_REVENDA, WCities.PRECO_MIN_REVENDA)
  }

  def secondSourceDf: DataFrame = {
    Seq(
      ("SUDESTE", "RIO DE JANEIRO", "GASOLINA", 10.000),
      ("SUDESTE", "RIO DE JANEIRO", "GASOLINA", 11.000),
      ("SUDESTE", "RIO DE JANEIRO", "ETANOL HIDRATADO", 12.000),
      ("SUDESTE", "RIO DE JANEIRO", "ETANOL HIDRATADO", 14.000),
      ("SUDESTE", "SÃO PAULO", "GASOLINA", 10.000),
      ("SUDESTE", "SÃO PAULO", "GASOLINA", 11.000),
      ("SUDESTE", "SÃO PAULO", "ETANOL HIDRATADO", 12.000),
      ("SUDESTE", "SÃO PAULO", "ETANOL HIDRATADO", 14.000),
      ("NORDESTE", "BAHIA", "GASOLINA", 10.000),
      ("NORDESTE", "BAHIA", "GASOLINA", 11.000),
      ("NORDESTE", "BAHIA", "ETANOL HIDRATADO", 12.000),
      ("NORDESTE", "BAHIA", "ETANOL HIDRATADO", 14.000)
    ).toDF(WCities.REGIAO, WCities.ESTADO, WCities.PRODUTO, WCities.PRECO_MEDIO_DISTRIBUICAO)
  }

  def thirdSourceDf: DataFrame = {
    Seq(
      ("30/05/2020", "CAMPINAS", 10.000, 15.000),
      ("30/05/2020", "CAMPINAS", 11.000, 16.000),
      ("30/05/2020", "CAMPINAS", 12.000, 17.000),
      ("30/07/2020", "SÃO PAULO", 10.000, 15.000),
      ("30/07/2020", "SÃO PAULO", 11.000, 16.000),
      ("30/07/2020", "SÃO PAULO", 12.000, 17.000)
    ).toDF(WCities.DATA_FINAL, WCities.MUNICIPIO, WCities.PRECO_MAX_REVENDA, WCities.PRECO_MIN_REVENDA)
  }

  def fourthSourceDf: DataFrame = {
    Seq(
      ("SÃO PAULO", 16.000, 1.000),
      ("CAMPINAS", 15.000, 2.000),
      ("JAGUARIÚNA", 17.000, 9.000),
      ("PEDREIRA", 15.000, 6.000),
      ("AMPARO", 12.000, 5.000),
      ("MONTE ALEGRE DO SUL", 20.000, 1.000)
    ).toDF(WCities.MUNICIPIO, WCities.PRECO_MAX_REVENDA, WCities.PRECO_MIN_REVENDA)
  }


}
