import configparser
from io import StringIO
import pytest
from readport import load_config, ConfigurationError


def test_load_config():
    """Check that the loaded configuration options have correct values and data types
    """
    config = r"""
        [device]
        station_name = MSU
        device_name = Test1
        host = 127.0.0.1
        port = 4001
        timeout = 30

        [parser]
        regex = ^x= *(\S+) y= *(\S+) z= *(\S+) T= *(\S+).*$
        var_names = u v w temp
        multiplier = 1
        pack_length = 12000
        destination = ./data/${device:station_name}_${device:device_name}_${date}.npz
        date_format = %Y-%m-%d_%H-%M-%S

        [logging]
        log_level = DEBUG
        log_file = readport_${device:port}.log
    """

    with StringIO(config) as f:
        conf = load_config(f)

    assert conf.station_name == "MSU"
    assert conf.device_name == "Test1"
    assert conf.host == "127.0.0.1"
    assert conf.port == 4001
    assert conf.timeout == 30
    assert conf.regex == br"^x= *(\S+) y= *(\S+) z= *(\S+) T= *(\S+).*$"
    assert conf.var_names == ["u", "v", "w", "temp"]
    assert conf.multiplier == 1
    assert conf.pack_length == 12000
    assert conf.destination == "./data/MSU_Test1_{date:%Y-%m-%d_%H-%M-%S}.npz"
    assert conf.log_level == "DEBUG"
    assert conf.log_file == "readport_4001.log"


def test_missing_setting():
    """Ensure that missing required options trigger an exception
    """

    config = r"""
        [device]

        [parser]

        [logging]
    """
    with StringIO(config) as f:
        with pytest.raises(configparser.NoOptionError):
            load_config(f)


def test_config_no_timeout():
    """Check that a commented out "timeout" yields timeout=None.
    """
    config = r"""
        [device]
        station_name = MSU
        device_name = Test1
        host = 127.0.0.1
        port = 4001
        #timeout = 30

        [parser]
        regex = ^x= *(\S+) y= *(\S+) z= *(\S+) T= *(\S+).*$
        var_names = u v w temp
        multiplier = 1
        pack_length = 12000
        destination = ./data/${device:station_name}_${device:device_name}_${date}.npz
        date_format = %Y-%m-%d_%H-%M-%S

        [logging]
        log_level = DEBUG
        log_file = readport_${device:port}.log
    """

    with StringIO(config) as f:
        conf = load_config(f)

    assert conf.timeout is None


def test_reserved_varname():
    """Check that the reserved variable name "time" in var_names triggers an exception
    """
    config = r"""
        [device]
        station_name = MSU
        device_name = Test1
        host = 127.0.0.1
        port = 4001
        timeout = 30

        [parser]
        regex = ^x= *(\S+) y= *(\S+) z= *(\S+) T= *(\S+).*$
        var_names = u v w temp time
        multiplier = 1
        pack_length = 12000
        destination = ./data/${device:station_name}_${device:device_name}_${date}.npz
        date_format = %Y-%m-%d_%H-%M-%S

        [logging]
        log_level = DEBUG
        log_file = readport_${device:port}.log
    """

    with StringIO(config) as f:
        with pytest.raises(ConfigurationError):
            load_config(f)


def test_regex_varnames_mismatch():
    """Ensure that an obvious mismatch between the number of regex capture groups and
    the number of variable names raises an exception.

    Config loader will not identify a mismatch if optional capture groups are present.
    """
    config = r"""
        [device]
        station_name = MSU
        device_name = Test1
        host = 127.0.0.1
        port = 4001
        timeout = 30

        [parser]
        regex = ^x= *(\S+) y= *(\S+) z= *(\S+) T= *(\S+).*$
        var_names = u v w temp extra
        multiplier = 1
        pack_length = 12000
        destination = ./data/${device:station_name}_${device:device_name}_${date}.npz
        date_format = %Y-%m-%d_%H-%M-%S

        [logging]
        log_level = DEBUG
        log_file = readport_${device:port}.log
    """

    with StringIO(config) as f:
        with pytest.raises(ConfigurationError):
            load_config(f)


def test_regex_invalid():
    """Ensure that an invalid regex raises an exception.
    """
    config = r"""
        [device]
        station_name = MSU
        device_name = Test1
        host = 127.0.0.1
        port = 4001
        timeout = 30

        [parser]
        regex = ^x= *(\S+) y= *(\S+) z= *(\S+) T= *(\S+
        var_names = u v w temp extra
        multiplier = 1
        pack_length = 12000
        destination = ./data/${device:station_name}_${device:device_name}_${date}.npz
        date_format = %Y-%m-%d_%H-%M-%S

        [logging]
        log_level = DEBUG
        log_file = readport_${device:port}.log
    """

    with StringIO(config) as f:
        with pytest.raises(ConfigurationError):
            load_config(f)
