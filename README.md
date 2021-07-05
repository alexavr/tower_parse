# Eddy Covariance Tower: Parsing Sensor Data

Extract measurements from streaming meteorological devices mounted on an [eddy covariance](https://en.wikipedia.org/wiki/Eddy_covariance) tower. Upload atmospheric data and system status to the central server for batch processing.

The system is operational at the Meteorological Observatory of the Moscow State University. Current hardware platform: a 32-bit ARMv7-based ASUS Tinker Board. The main process parses binary data streams in real-time according to the rules for each device. We provide configuration files for the deployed sonic anemometers and humidity/temperature probes.

## Requirements

To get started, ensure that you have Python 3.6 or later. Additional dependencies include NumPy, which on a Debian-based machine can be installed by running:

```shell
$ sudo apt install python3-numpy
```

Advanced regular expressions are supported by an *optional* 3rd-party [regex](https://pypi.org/project/regex/) module. These may be useful when working with multiple devices connected to one TCP port through a data logger. In case the message format differs drastically between devices, regular expressions involving capture groups with the same name might be beneficial for unified processing. At present, this functionality isn't used, so the additional dependency can be safely ignored:

```shell
$ sudo apt install python3-regex  # optional, needed for advanced regex functionality
```

Alternatively, use the production conda environment:

```shell
$ conda env create -f environment.yml
$ conda activate
```

## Usage

1. **Collect data samples:** To set up a new device, first identify the format of messages sent over the TCP socket. We expect each message from the device to end with a newline. Save and inspect binary messages from the device by running (for a specific IP address and port number):

   ```shell
   $ ./readport.py --echo 192.168.192.48:4005 > data.bin
   ```

   Press Ctrl-C to terminate the script in a couple of seconds.

2. **Create a config file:** Study the resulting `data.bin` file and come up with a regular expression for extracting variables of interest from binary data. You may find `less`,  `hexdump -C`, and [regex101.com](https://regex101.com/) useful at this stage. Create a configuration file for your device similar to the other `readport_400N.conf` files in this repo.

3. **Validate:** Test the new configuration file in debug mode. If your regular expression is correct, you will see a list of extracted variables and their values. The current time of each message is automatically recorded as the `time` variable (expressed in seconds since the Unix epoch):

   ```shell
   $ ./readport.py --config readport_4005.conf --debug
   INFO  Connected to 192.168.192.48:4005. Ready to receive device data...
   DEBUG Got {'u': 0.079, 'temp': 14.94, 'time': 1610713847.4186084}
   DEBUG Got {'u': 0.081, 'temp': 15.03, 'time': 1610713847.9193459}
   ```

   Again, Ctrl-C will terminate the script.

4. **Launch in production:** If you're satisfied with the results, rerun the previous command without the `--debug` argument within `screen` to start data collection. You might also want to add the script to crontab to run on system reboot, e.g.:

   ```shell
   @reboot screen -d -m /path/to/readport.py --config /path/to/readport_4005.conf
   ```

## Additional crontab settings

```bash
*/1 * * * *  /path/to/hb_client.sh &>/dev/null
*/30 * * * * /path/to/send_data.sh &>/dev/null
```

## File description

* `hb_client.sh` — reads system status (CPU temperature, RAM usage, traffic, etc.) and sends it to the central server (TowerServer)
* `send_data.sh` — upload the files with meteorological data to the server for post-processing.
* `readport.py` — reads data from meteorological devices. These are connected via Ethernet on ports 4001-4004 (TCP) of a Moxa NPort server. The detailed settings of each connection are supplied through the corresponding configuration file.
* `readport_400N.conf` — the configuration files for each device. The device names, port numbers, and the logic for parsing binary messages vary between devices.
* `extras/fake_server.py` — a simulated server that sends messages in the appropriate format for ad-hoc testing of `readport.py`
* `extras/debug.conf` — a configuration file for use with `fake_server.py`

## Contributing code

We welcome your code contributions! Before creating a pull request, please run the test suite:

```shell
$ conda env create -f tests/environment.yml
$ conda activate tower_parse
$ pytest
```

Make sure that all tests pass or are automatically skipped. We expect the code to be formatted with [black](https://github.com/psf/black). Some tests will fail if it's not.
