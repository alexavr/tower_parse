#!/bin/bash
# crontab -e -> /home/gavr/TowerMSU/send_data.sh
rsync -a --exclude='*.tmp' --remove-source-files /home/gavr/TowerMSU/data/ naad-tower:/public/TOWER/npz/