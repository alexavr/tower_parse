import random
import time
from collections import defaultdict
from pathlib import Path
import numpy as np
import pytest
from readport import Item, Parser


def test_parser_extract_ok():
    """Check that well-formed inputs produce the correct extracted values
    """
    data = b"\x02Q,+000.079,-000.102,+000.095,M,+014.94,0000001,\x030F\r\n"
    regex = br"^.+,([^,]+),([^,]+),([^,]+),.,([^,]+),.+$"
    var_names = "u v w temp".split()
    multiplier = 10  # for the sake of the test only
    timestamp = time.time()
    expected = dict(u=0.79, v=-1.02, w=0.95, temp=149.4, time=timestamp)

    item = Item(data, timestamp, False)
    parser = Parser(regex, var_names, multiplier, None, None)
    got = parser.extract(item)
    assert got == expected


def test_parser_extract_incomplete(caplog):
    """Ensure that an incomplete message results in no match and raises an exception
    """
    data = b"M,+014.94,0000001,\x030F\r\n"
    regex = br"^.+,([^,]+),([^,]+),([^,]+),.,([^,]+),.+$"
    var_names = "u v w temp".split()
    multiplier = 1

    parser = Parser(regex, var_names, multiplier, None, None)
    with pytest.raises(AttributeError):
        item = Item(data, time.time(), True)
        parser.extract(item)

    log = [
        rec.message
        for rec in caplog.records
        if "Possibly incomplete first message" in rec.message
    ]
    assert len(log) == 1

    with pytest.raises(AttributeError):
        item = Item(data, time.time(), False)
        parser.extract(item)

    log = [
        rec.message
        for rec in caplog.records
        if "Cannot parse a complete message" in rec.message
    ]
    assert len(log) == 1


def test_parser_extract_cast_error():
    """Check that floating point conversions trigger an exception
    """
    data = b"\x02Q,ZZZ+000.079,-000.102,+000.095,M,+014.94,0000001,\x030F\r\n"
    regex = br"^.+,([^,]+),([^,]+),([^,]+),.,([^,]+),.+$"
    var_names = "u v w temp".split()
    multiplier = 1

    item = Item(data, time.time(), False)
    parser = Parser(regex, var_names, multiplier, None, None)
    with pytest.raises(ValueError):
        parser.extract(item)


def test_parser_extract_mismatch():
    """Verify that a mismatch between the number of extracted values and var_names
    raises a parse error
    """
    data = b"\x02Q,+000.079,-000.102,+000.095,M,+014.94,0000001,\x030F\r\n"
    regex = br"^.+,([^,]+),([^,]+),([^,]+),.,([^,]+),.+$"
    var_names = "u v w temp extra".split()
    multiplier = 1

    item = Item(data, time.time(), False)
    parser = Parser(regex, var_names, multiplier, None, None)
    with pytest.raises(AssertionError):
        parser.extract(item)


def test_parser_write_ok(tmp_path):
    """Ensure that files are written properly
    """
    var_names = "u v w temp".split()
    all_vars = var_names + ["time"]
    pack_length = 2

    # Use microseconds and a unique file identifier
    destination = str(tmp_path / "data" / "MSU_Test1_{date:%H-%M-%S-%f}.npz")
    microsecond = 0.000001

    # Two complete files expected as output:
    n_iters = 2
    buffers = [defaultdict(list) for _ in range(n_iters)]

    parser = Parser(
        regex=None,
        var_names=var_names,
        multiplier=None,
        pack_length=pack_length,
        destination=destination,
    )

    for i in range(n_iters):
        for _ in range(pack_length):
            variables = {var: random.uniform(-10, 10) for var in all_vars}
            parser.write(variables)

            # Form an expected representation of the data
            for var, value in variables.items():
                buffers[i][var].append(value)

        # Make sure that the whole iteration is at least 1 microsecond long
        time.sleep(microsecond)

    files = sorted([str(p) for p in tmp_path.glob("**/*") if p.is_file()])

    assert len(files) == n_iters
    for i, file in enumerate(files):
        with np.load(file) as data:
            for var in all_vars:
                expected = np.array(buffers[i][var])
                assert np.array_equal(data[var], expected)
                assert data[var].dtype == expected.dtype


def test_parser_write_inconsistent_vars(tmp_path):
    """Check that supplying a wrong set of variables triggers and exception
    """
    var_names = ["u", "v", "w"]
    all_vars = var_names + ["time"]
    variables = {var: 1.0 for var in all_vars}
    # Remove one of the variables, which should cause an error
    del variables["u"]

    pack_length = 2
    destination = str(tmp_path / "data" / "MSU_Test1_{date:%H-%M-%S-%f}.npz")

    parser = Parser(
        regex=None,
        var_names=var_names,
        multiplier=None,
        pack_length=pack_length,
        destination=destination,
    )
    with pytest.raises(AssertionError):
        parser.write(variables)


def test_parser_write_mkdir_failed():
    """Test that failing at filesystem operations raises an error
    """
    var_names = ["u", "v", "w"]
    all_vars = var_names + ["time"]
    variables = {var: 1.0 for var in all_vars}

    pack_length = 1
    # Use the /TEST directory to cause a permission problem
    destination = str(Path("/") / "TEST" / "MSU_Test1_{date:%H-%M-%S-%f}.npz")

    parser = Parser(
        regex=None,
        var_names=var_names,
        multiplier=None,
        pack_length=pack_length,
        destination=destination,
    )
    with pytest.raises(OSError):
        parser.write(variables)
