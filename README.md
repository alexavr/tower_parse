# tower_client

The settings of Small PC (currently Asus Tinker Board on ARMv7) for reading data from several sonic anemometers. Anemometers are mounted on meteorological tower. Also scripts for reading and sending the data and some system info to the server (TowerServer).

## Crontab settings:
```bash
*/1 * * * * /home/gavr/TowerMSU/hb_client.sh >/dev/null 2>&1
*/30 * * * * /home/gavr/TowerMSU/send_data.sh >/dev/null 2>&1
```

## File description:
* `hb_client.sh` - reads system status (CPU temperature, RAM usage, Traffic etc) and sends it to TowerServer.
* `readport_400N.py` - reads sonic data, 4001-4004 are Ethernet ports on MOXA.
* `readport_400N_light.py` - CPU-not-heavy (light) versions of `readport_400N.py`: no accumulating, saving in binary format (instead on npz). Aimed to check the performance and stability.
* `readport_400N_light_dv.py` - reads 3 packets and saves it in binary file. Aimed to check the format, stop-bits etc.
* `readport_400N_firstGavr.py` - the first (read old) version of `readport_400N.py` (don't need it).

