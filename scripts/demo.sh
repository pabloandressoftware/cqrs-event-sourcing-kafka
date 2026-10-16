#!/usr/bin/env bash
# Demo of the event replay test (Part 2 of the workshop).
#
#   ./scripts/demo.sh before   -> original code (4 topics): replay loses events
#   ./scripts/demo.sh after    -> new code (1 topic): replay keeps the order
#
# Requires the docker-compose containers running and NO account.cmd / account.query started by hand.
# WARNING: wipes the bankAccount data in MongoDB, MySQL and Kafka (local test data only).
set -u
MODE=${1:-}
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
case "$MODE" in
  before) BA="$ROOT/../original/bank-account"; TOPICS="AccountOpenedEvent FundsDepositedEvent FundsWithdrawnEvent AccountClosedEvent" ;;
  after)  BA="$ROOT/bank-account";             TOPICS="BankAccountEvents" ;;
  *) echo "uso: $0 before|after"; exit 1 ;;
esac
LOGS=$(mktemp -d)
CMD=http://localhost:5000/api/v1
QRY=http://localhost:5001/api/v1/bankAccountLookup
KAFKA="docker exec kafka /opt/kafka/bin"
MYSQL="docker exec mysql mysql -uroot -ptechbankRootPsw"

title() { echo; echo -e "\e[1;36m==== $* ====\e[0m"; }
run()   { echo -e "\e[1;32m\$ $*\e[0m"; eval "$@"; }
snap()  { echo; read -rp $'\e[1;33m>>> Toma la captura de pantalla y pulsa Enter para continuar...\e[0m'; clear; }
wait_log() { until grep -q "$2" "$1" 2>/dev/null; do sleep 1; done; }
open_acc() { curl -s -X POST $CMD/openBankAccount -H 'Content-Type: application/json' \
  -d "{\"accountHolder\":\"$1\",\"accountType\":\"$2\",\"openingBalance\":$3}" | sed -E 's/.*"id":"([^"]+)".*/\1/'; }
money() { curl -s -o /dev/null -w "  PUT $1 $3 -> HTTP %{http_code}\n" -X PUT $CMD/$1/$2 -H 'Content-Type: application/json' -d "{\"amount\":$3}"; }
start_query() { (cd "$BA" && java -jar account.query/target/account.query-1.0.0.jar > "$LOGS/query.log" 2>&1 &) ;
  wait_log "$LOGS/query.log" "partitions assigned"; sleep 3; }
stop_query() { pkill -f "^java -jar account.query" ; sleep 3; }
cleanup() { pkill -f "^java -jar account.(cmd|query)" ; }
trap cleanup EXIT

pkill -f "^java -jar account.(cmd|query)"; sleep 2
clear
title "Preparando entorno limpio ($MODE)"
docker exec mongodb mongo bankAccount --quiet --eval 'db.dropDatabase()' >/dev/null
$MYSQL -e "drop database if exists bankAccount" 2>/dev/null
$KAFKA/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --delete --group bankaccConsumer >/dev/null 2>&1
for t in AccountOpenedEvent FundsDepositedEvent FundsWithdrawnEvent AccountClosedEvent BankAccountEvents; do
  $KAFKA/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic $t >/dev/null 2>&1; done
sleep 3
for t in $TOPICS; do $KAFKA/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic $t --partitions 1 --replication-factor 1 >/dev/null; done
(cd "$BA" && java -jar account.cmd/target/account.cmd-1.0.0.jar > "$LOGS/cmd.log" 2>&1 &)
wait_log "$LOGS/cmd.log" "Started CommandApplication"
start_query
echo "Command API y Query API arriba ($( [ $MODE = before ] && echo 'código ORIGINAL' || echo 'código NUEVO'))."

title "1. Operaciones: abrir cuentas, depositar, retirar, cerrar"
run "$KAFKA/kafka-topics.sh --bootstrap-server localhost:9092 --list | grep -v __"
M=$(open_acc "Maria Lopez" CURRENT 250);  echo "  POST openBankAccount Maria Lopez (250)  -> $M"
C=$(open_acc "Carlos Ruiz" SAVINGS 5000); echo "  POST openBankAccount Carlos Ruiz (5000) -> $C"
P=$(open_acc "Pedro Diaz" SAVINGS 800);   echo "  POST openBankAccount Pedro Diaz (800)   -> $P"
for i in 1 2 3; do money depositFunds $M 100; money depositFunds $C 1000; done
money withdrawFunds $C 500
money depositFunds $P 200
curl -s -o /dev/null -w "  DELETE closeBankAccount Pedro -> HTTP %{http_code}\n" -X DELETE $CMD/closeBankAccount/$P
sleep 3
echo; echo "Esperado: Maria 250+3x100 = 550 | Carlos 5000+3x1000-500 = 7500 | Pedro cerrada"
run "$MYSQL -t -e 'select account_holder, balance from bankAccount.bank_account' 2>/dev/null"
snap

title "2. Event Store (MongoDB) = fuente de verdad"
run "docker exec mongodb mongo bankAccount --quiet --eval 'db.eventStore.find().sort({timeStamp:1}).forEach(e => print(\"v\" + e.version + \"  \" + e.eventType.split(\".\").pop().padEnd(20) + \" \" + e.aggregateIdentifier))'"
snap

title "3. Reinicio del servicio de consulta con replay desde el offset 0"
echo "Deteniendo account.query..."; stop_query
run "$MYSQL -e 'drop database bankAccount' 2>/dev/null && echo 'read DB borrada'"
run "$KAFKA/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group bankaccConsumer --reset-offsets --to-earliest --all-topics --execute"
echo; echo -e "\e[1;32m\$ java -jar account.query/target/account.query-1.0.0.jar\e[0m"
start_query; sleep 3
grep -E "partitions assigned|Consuming" "$LOGS/query.log" | sed -E 's/^[0-9]{4}-[0-9]{2}-[0-9]{2} //; s/ +INFO [0-9]+ --- / /; s/ +: / : /'
echo; run "curl -s $QRY/ | python3 -m json.tool"
echo; run "$MYSQL -t -e 'select account_holder, balance from bankAccount.bank_account' 2>/dev/null"
echo "Esperado: Maria 550 | Carlos 7500"
[ "$MODE" = before ] && echo "(el código original además no aplica los retiros en el read model: aun en orden daría Carlos 8000)"
snap

if [ "$MODE" = after ]; then
  title "4. POST /restoreReadDb con la read DB vacía"
  run "$MYSQL -e 'delete from bankAccount.bank_account' 2>/dev/null && echo 'read DB vaciada'"
  N=$(wc -l < "$LOGS/query.log")
  run "curl -s -X POST $CMD/restoreReadDb; echo"
  sleep 3
  tail -n +"$N" "$LOGS/query.log" | grep Consuming | sed -E 's/^[0-9]{4}-[0-9]{2}-[0-9]{2} //; s/ +INFO [0-9]+ --- / /; s/ +: / : /'
  echo; run "$MYSQL -t -e 'select account_holder, balance from bankAccount.bank_account' 2>/dev/null"
  snap
fi
echo "Demo '$MODE' terminada. Se detienen los servicios lanzados por el script."
