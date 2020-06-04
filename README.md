# tower_client

## Crontab settings:
```bash
*/1 * * * * /home/gavr/TowerMSU/hb_client.sh >/dev/null 2>&1
*/30 * * * * /home/gavr/TowerMSU/send_data.sh >/dev/null 2>&1
```

## File description:

`hb_client.sh` - reads system status (health) and sends it to TowerServer.

`readport_400N.py` - reads sonic data, 4001-4003 are Ethernet ports on MOXA.

`readport_400N_light.py` - CPU not heavy (light) versions of `readport_400N.py`: no accumulating, saving in binary format. Aimed to check the performance and stability.

`readport_400N_light_dv.py` - reads first 3 packets and saves it in binary. Aimed to check the format, stop-bits etc.

`readport_400N_firstGavr.py` - first version of `readport_400N.py` (don't need it).

