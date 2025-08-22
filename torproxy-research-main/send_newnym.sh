#!/bin/bash

LOGFILE="/var/log/send_newnym.log"

echo "Running send_newnym.sh at $(date)" >> $LOGFILE

# Send the NEWNYM signal to the Tor proxy
{
    echo -e 'AUTHENTICATE "!tudbw8921"\nSIGNAL NEWNYM\nQUIT' | nc 127.0.0.1 9051
    echo "Sent NEWNYM signal" >> $LOGFILE

    torify wget -qO- http://icanhazip.com > /tmp/ip_output &
    wget_pid=$!

    wait $wget_pid

    ip=$(cat /tmp/ip_output)

    if [ -z "$ip" ]; then
        echo "Failed to fetch IP" >> "$LOGFILE"
    else
        echo "Fetched Changed IP : $ip" >> "$LOGFILE"
    fi
} >> $LOGFILE 2>&1

if ps -ef | grep -v 'grep' | grep -q tor; then 
    echo "Tor is running" >> $LOGFILE
    exit 0
else
    echo "Tor is not running" >> $LOGFILE
    exit 1
fi
