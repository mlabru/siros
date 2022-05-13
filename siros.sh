#!/bin/bash

# language
# export LANGUAGE=pt_BR

# odin directory
ODIN=~/odin

# nome do computador
HOST=`hostname`

# get today's date
TDATE=`date '+%Y-%m-%d_%H-%M-%S'`

# home directory exists ?
if [ -d ${ODIN} ]; then
    # set home dir
    cd ${ODIN}
fi

# ckeck if another instance of loader is running
DI_PID_MONITOR=`ps ax | grep -w python3 | grep -w siros_mon.py | awk '{ print $1 }'`

if [ ! -z "$DI_PID_MONITOR" ]; then
    # log warning
    echo "[`date`]: process monitor is already running. Restarting..."
    # kill process
    kill -9 $DI_PID_MONITOR
    # wait 3s
    sleep 3
fi

# set PYTHONPATH
export PYTHONPATH="$PWD/."

# executa o monitor  
python3 siros/siros_mon.py $@ > logs/siros_mon.$HOST.$TDATE.log 2>&1 &

# < the end >----------------------------------------------------------------------------------
