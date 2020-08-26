# eleflow-test

### Passo a passo para execução:

1. Clone o repositório.
2. Crie um arquivo de configuração que será passado para aplicação (Exemplo abaixo).
3. No diretório onde o código foi clonado, executar o script "build-exec.sh" passando como argumento o caminho para o arquivo de configuração. Por exemplo:
~~~ 
$ ./buil-exec.sh /home/user/config.json
ou
$ ./exec.sh /home/user/config.json
~~~

Obs: O script "build-exec.sh" faz o build da aplicação via *mvn* e em seguida executa ela via *spark-submit*. O script "exec.sh" apenas executa a aplicação via spark-submit.
    

### Exemplo do arquivo de configuração:
~~~
{
  "inputPath": "/home/user/ARQUIVO.csv",            //Caminho da base de dados
  "show": true,                                     //Booleano para indicar se os resultados aparecerão no log 
  "outputPath": "/home/user/results/"               //Parâmetro opicional - Caminho onde os resultados poderão ser salvos
}
~~~


  
