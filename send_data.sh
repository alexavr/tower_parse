#!/bin/bash
# crontab -e -> /home/gavr/TowerMSU/send_data.sh

SOURCE=/home/tower/tower_parse/data/
DEST=data-tower:/var/www/data/domains/tower.ocean.ru/html/flask/data/npz/

(
    flock -n 9 || exit 1
    rsync -a --exclude='*.tmp' --remove-source-files "$SOURCE" "$DEST"
) 9>/tmp/send_data.lock
