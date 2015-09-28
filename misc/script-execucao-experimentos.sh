#!/bin/bash

if [ $# -ne 1 ]; then
	echo "Sintaxe: $0 <criptografado:true/false>"
	exit 1
fi

cd `dirname $0`

CAMINHO_KAFKA="/opt/kafka"
CAMINHO_STORM="/opt/storm"
PASTA_APLICACAO=`dirname $(pwd)`
PASTA_DADOS_ZOOKEEPER="/opt/zookeeper/data"
PASTA_DADOS_KAFKA="/tmp/kafka-logs"
CAMINHO_JAR="$PASTA_APLICACAO/target/calculo-estatisticas-consumo-1.0-SNAPSHOT-jar-with-dependencies.jar"
CLASSE_MAIN="br.edu.ufcg.copin.storm.estatisticas.CalculoMediaMovelTopology"
HOST_STORM="guest01"
HOST_BROKER="guest01"
PORTA_BROKER="9092"
HOSTS_SSH="guest01 guest02 guest03 guest04"
PASTA_LOGS="$HOME/logs-experimentos/$$"
TAXAS_ENTRADA="250 500 1000 2000"
TOPICO="testes-storm"
HOST_METRICAS="guest03"
CAMINHO_LOG_METRICAS="${CAMINHO_STORM}/logs/metrics.log"
NUMERO_REPETICOES=5
NUMERO_PARTICOES=3
FATOR_REPLICACAO=1
NUMERO_SPOUTS=3
NUMERO_BOLTS=3
PERCENTUAL_SLEEP=1.0
TEMPO_EXECUCAO=600
CRIPTOGRAFADO=$1

for HOST in $HOSTS_SSH; do
    ssh $HOST killall java
    sleep 1
    ssh $HOST killall -9 java

    if [ "$HOST" == "$HOST_STORM" ]; then
        ssh $HOST rm -fr ${PASTA_DADOS_KAFKA} ${PASTA_DADOS_ZOOKEEPER}
        ssh $HOST mkdir -p ${PASTA_DADOS_KAFKA} ${PASTA_DADOS_ZOOKEEPER}
    fi

    if [ "$HOST" == "$HOST_BROKER" ]; then
        ssh $HOST start-kafka-cluster
    fi

    ssh $HOST start-storm-cluster
done

sleep 20

rm -f $PASTA_LOGS/*
mkdir -p $PASTA_LOGS

cd $PASTA_APLICACAO
git pull

mvn clean package

for TAXA_ENTRADA in $TAXAS_ENTRADA; do
    for (( REPETICAO = 1; REPETICAO <= $NUMERO_REPETICOES; REPETICAO++ )); do
        ssh $HOST_METRICAS rm $CAMINHO_LOG_METRICAS
        ssh $HOST_METRICAS touch $CAMINHO_LOG_METRICAS

        for HOST in $HOSTS_SSH; do
            nohup ssh $HOST jvmtop.sh &> $PASTA_LOGS/jvmtop-${HOST}-${TAXA_ENTRADA}-${REPETICAO}.log &
        done

        TOPICO_ATUAL=${TOPICO}-${TAXA_ENTRADA}-$$-${REPETICAO}
        $CAMINHO_KAFKA/bin/kafka-topics.sh --zookeeper ${HOST_ZOOKEEPER}:${PORTA_ZOOKEEPER} --create --topic $TOPICO_ATUAL --partitions $NUMERO_PARTICOES --replication-factor $FATOR_REPLICACAO
    	$CAMINHO_STORM/bin/storm jar $CAMINHO_JAR $CLASSE_MAIN $HOST_BROKER $PORTA_BROKER $TOPICO_ATUAL $NUMERO_SPOUTS $NUMERO_BOLTS $TAXA_ENTRADA $PERCENTUAL_SLEEP $CRIPTOGRAFADO $TEMPO_EXECUCAO &> $PASTA_LOGS/storm-${TAXA_ENTRADA}-${REPETICAO}.log

        for HOST in $HOSTS_SSH; do
            ssh $HOST pkill -f -9 jvmtop
        done

        scp $HOST_METRICAS:$CAMINHO_LOG_METRICAS $PASTA_LOGS/metricas-${TAXA_ENTRADA}-${REPETICAO}.log

        sleep 15
    done
done
