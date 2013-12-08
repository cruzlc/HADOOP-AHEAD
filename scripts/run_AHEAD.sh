#!/bin/bash
function isNumeric () { 
    local var="$1" 
     echo "$var" | grep -q -v "[^0-9]"  && return 0 || return 1  
}

# Verifica validade do comando de entrada.
if ( [[ "$1" != "-r" ]] || ( ! isNumeric "$2" ) || [[ "$2" == "" ]] || [[ "$3" != "-c" ]] || [[ ! -e "$4" ]] )
 then
  echo -e "\n"
  echo "Uso: $0 -r <numero de reducers> -c <caminho do arquivo>"
  echo -e "\n"
  exit
fi

# Apaga dados (grafo) no HDFS.
/opt/hadoop/bin/hadoop fs -rmr /user/hdfs/grafo
# Coloca novos dados de entrada no HDFS (grafo).
/opt/hadoop/bin/hadoop fs -put $4 /user/hdfs/grafo
# Executa o AHEAD.
time /opt/hadoop/bin/hadoop jar AHEAD.jar AHEAD.BFS_Paralelo $2 /user/hdfs/grafo