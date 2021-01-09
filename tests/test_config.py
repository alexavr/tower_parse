import configparser
import importlib
from io import StringIO
import pytest
import readport
from readport import load_config, ConfigurationError


def test_load_config():
    """Check that the loaded configuration options have correct values and data types
    """
    config = r"""
        [device]
        station = MSU
        name = Test1
        host = 127.0.0.1
        port = 4001
        timeout = 30

        [parser]
        regex = ^x= *(?P<u>\S+) y= *(?P<v>\S+) z= *(?P<w>\S+) T= *(?P<temp>\S+).*$
        pack_length = 12000
        destination = ./data/

        [logging]
        level = DEBUG
        file = readport_${device:port}.log
    """

    with StringIO(config) as f:
        conf = load_config(f)

    assert conf.station == "MSU"
    assert conf.device == "Test1"
    assert conf.host == "127.0.0.1"
    assert conf.port == 4001
    assert conf.timeout == 30
    assert (
        conf.regex
        == br"^x= *(?P<u>\S+) y= *(?P<v>\S+) z= *(?P<w>\S+) T= *(?P<temp>\S+).*$"
    )
    assert conf.pack_length == 12000
    assert conf.dest_dir == "./data/"
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
        station = MSU
        name = Test1
        host = 127.0.0.1
        port = 4001
        #timeout = 30

        [parser]
        regex = ^x= *(?P<u>\S+) y= *(?P<v>\S+) z= *(?P<w>\S+) T= *(?P<temp>\S+).*$
        pack_length = 12000
        destination = ./data/

        [logging]
        level = DEBUG
        file = readport_${device:port}.log
    """

    with StringIO(config) as f:
        conf = load_config(f)

    assert conf.timeout is None


def test_reserved_varname():
    """Check that the reserved variable name "time" in var_names triggers an exception
    """
    config = r"""
        [device]
        station = MSU
        name = Test1
        host = 127.0.0.1
        port = 4001
        timeout = 30

        [parser]
        regex = ^x= *(?P<u>\S+) y= *(?P<v>\S+) z= *(?P<w>\S+) T= *(?P<time>\S+).*$
        pack_length = 12000
        destination = ./data/

        [logging]
        level = DEBUG
        file = readport_${device:port}.log
    """

    with StringIO(config) as f:
        with pytest.raises(ConfigurationError):
            load_config(f)


def test_regex_invalid():
    """Ensure that an invalid regex raises an exception.
    """
    config = r"""
        [device]
        station = MSU
        name = Test1
        host = 127.0.0.1
        port = 4001
        timeout = 30

        [parser]
        regex = ^x= *(?P<u>\S+) y= *(?P<v>\S+) z= *(?P<w>\S+) T= *(?P<temp>\S+.*$
        pack_length = 12000
        destination = ./data/

        [logging]
        level = DEBUG
        file = readport_${device:port}.log
    """

    with StringIO(config) as f:
        with pytest.raises(ConfigurationError):
            load_config(f)


def test_regex_not_named():
    """Check that any unnamed regex capture groups trigger an error.
    """
    config = r"""
        [device]
        station = MSU
        name = Test1
        host = 127.0.0.1
        port = 4001
        timeout = 30

        [parser]
        regex = ^x= *(?P<u>\S+) y= *(?P<v>\S+) z= *(?P<w>\S+) T= *(\S+).*$
        pack_length = 12000
        destination = ./data/

        [logging]
        level = DEBUG
        file = readport_${device:port}.log
    """

    with StringIO(config) as f:
        with pytest.raises(ConfigurationError):
            load_config(f)


def test_regex_no_advanced():
    """Test that advanced regex functionality, particularly capture groups with
    the same name:
        - raise an error if `regex` isn't installed
    """
    config = r"""
        [device]
        station = MSU
        name = Test1
        host = 127.0.0.1
        port = 4001
        timeout = 30

        [parser]
        regex = (?P<name>foo)|(?P<name>bar)
        pack_length = 12000
        destination = ./data/

        [logging]
        level = DEBUG
        file = readport_${device:port}.log
    """
    readport.re = importlib.import_module("re")
    with StringIO(config) as f:
        with pytest.raises(ConfigurationError):
            load_config(f)


def test_regex_advanced():
    """Test that advanced regex functionality, particularly capture groups with
    the same name:
        - pass the configuration check if `regex` is installed
    """
    config = r"""
        [device]
        station = MSU
        name = Test1
        host = 127.0.0.1
        port = 4001
        timeout = 30

        [parser]
        regex = (?P<name>foo)|(?P<name>bar)
        pack_length = 12000
        destination = ./data/

        [logging]
        level = DEBUG
        file = readport_${device:port}.log
    """
    pytest.importorskip("regex", reason="Please pip install regex")

    readport.re = importlib.import_module("regex")
    with StringIO(config) as f:
        load_config(f)  # no exception should be raised
