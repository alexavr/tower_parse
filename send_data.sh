#!/bin/bash
# crontab -e -> /home/gavr/TowerMSU/send_data.sh

SOURCE=/home/gavr/TowerMSU/data/
DEST=naad-tower:/var/www/data/domains/tower.ocean.ru/html/flask/data/npz/

rsync -a --exclude='*.tmp' --remove-source-files "$SOURCE" "$DEST"