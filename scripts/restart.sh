#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

print_header "ПЕРЕЗАПУСК ЛОР-ПОМОЩНИКА"

$SCRIPT_DIR/stop.sh
sleep 2
$SCRIPT_DIR/start.sh
