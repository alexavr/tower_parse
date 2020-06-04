#!/bin/bash
name="MSU"
datetime=$(date +%s)
temp=$(cat /sys/class/thermal/thermal_zone0/temp)
hdd=$(df -h | grep root | tr -s ' ' | cut -d' ' -f5 | cut -d'%' -f1)

mtot=$(echo $(cat /proc/meminfo | grep MemTotal) | cut -d' ' -f2)
mfre=$(echo $(cat /proc/meminfo | grep MemAvailable) | cut -d' ' -f2)
ram=$(( 100 - $mfre * 100 / $mtot ))

# in=$(netstat -i | grep eth0 | awk '{print $3}')
# out=$(netstat -i | grep eth0 | awk '{print $7}')
in=$(/sbin/ifconfig eth0 | grep bytes | grep RX | tr -s ' ' | cut -d' ' -f6)
out=$(/sbin/ifconfig eth0 | grep bytes | grep TX | tr -s ' ' | cut -d' ' -f6)

# echo "curl tower.domain/hp/?name=$name&datetime=$datetime&temp=$temp&hdd=$hdd&ram=$ram"
# echo "curl --data 'name=$name&datetime=$datetime&temp=$temp&hdd=$hdd&ram=$ram' http:/139.59.212.122:5000/hb"

# This goes to Digital Ocean server (old)
# curl --data "name=$name&datetime=$datetime&temp=$temp&hdd=$hdd&ram=$ram&in=$in&out=$out" http:/139.59.212.122:5000/hb_put
curl --data "name=$name&datetime=$datetime&temp=$temp&hdd=$hdd&ram=$ram&in=$in&out=$out" http:/tower.ocean.ru:5000/hb_put

