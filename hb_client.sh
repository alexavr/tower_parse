#!/bin/bash
name="PIO"
datetime=$(date +%s)
cputemp=$(cat /sys/class/thermal/thermal_zone0/temp)
hdd=$(df -h | grep mmcblk0p1 | tr -s ' ' | cut -d' ' -f5 | cut -d'%' -f1)

mtot=$(echo $(cat /proc/meminfo | grep MemTotal) | cut -d' ' -f2)
mfre=$(echo $(cat /proc/meminfo | grep MemAvailable) | cut -d' ' -f2)
ram=$(( 100 - $mfre * 100 / $mtot ))

# Thermo sensor
boxtemp=-999 # $(/usr/local/bin/RODOS --id 5519 --read | grep T= | cut -f3 -d'=')

# in=$(netstat -i | grep eth0 | awk '{print $3}')
# out=$(netstat -i | grep eth0 | awk '{print $7}')
in=$(/sbin/ifconfig eth0 | grep bytes | grep RX | tr -s ' ' | cut -d' ' -f6)
out=$(/sbin/ifconfig eth0 | grep bytes | grep TX | tr -s ' ' | cut -d' ' -f6)

echo "curl --data 'name=$name&datetime=$datetime&boxtemp=$boxtemp&cputemp=$cputemp&hdd=$hdd&ram=$ram&in=$in&out=$out' http://tower.ocean.ru:5000/hb_put"
curl --data "name=$name&datetime=$datetime&boxtemp=$boxtemp&cputemp=$cputemp&hdd=$hdd&ram=$ram&in=$in&out=$out" http://tower.ocean.ru:5000/hb_put

