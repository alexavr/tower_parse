#!/bin/bash
# crontab -e -> /home/gavr/TowerMSU/send_data.sh
for i in $(ls /home/gavr/TowerMSU/data/*.npz); do
	rsync -a ${i} naad-tower:/public/TOWER/npz/
	if [ "$?" -eq "0" ]; then
  		rm -rf ${i}
	fi
done
