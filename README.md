# eleflow-test

### Objetivo:

### Passo a passo para execução:

1. Clone o código disponível.
2. Crie um arquivo de configuração que será passado para aplicação.
3. No diretório onde o código foi clonado, executar o script "build-exec.sh" passando como argumento o caminho para o arquivo de configuração. Por exemplo:
~~~ 
$ ./buil-exec.sh /home/fernando.souza/minha-configuração.json
~~~

Obs: O script "build-exec.sh" faz o build da aplicação via mvn e em seguida executa ela via spark-submit. O script "exec.sh" apenas executa a aplicação via spark-submit.  
