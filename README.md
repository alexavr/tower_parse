# tower_client

The settings of a Small PC (currently Asus Tinker Board on ARMv7) for reading data from several sonic anemometers. Anemometers are mounted on a meteorological tower. Also, the scripts for reading and sending the data and some system info to the server (TowerServer).

## Crontab settings:
```bash
*/1 * * * *  /path/to/hb_client.sh >/dev/null 2>&1
*/30 * * * * /path/to/send_data.sh >/dev/null 2>&1
```

## File description:
* `hb_client.sh` - reads system status (CPU temperature, RAM usage, Traffic etc) and sends it to TowerServer.
* `readport.py` - reads sonic data. 4001-4004 are Ethernet ports on MOXA, supplied through the corresponding configuration files. Currently operational.
* `readport_400N.conf`: the configuration files for each sonic device. Only the port numbers differ at the moment.
* `extras/readport_400N_light.py` - a CPU-lightweight version of `readport.py`: no data accumulation, only saving timestamps in binary format (instead on .npz). Aimed to check script performance and system stability.
* `extras/readport_400N_format.py` - reads 3 messages from the device and saves them in binary file. Aimed to check the message format, delimiter bits, etc.
* `extras/readport_400N_firstGavr.py` - the first (read old) version of `readport_400N.py` (not needed).

