import pytest

from good_practices import taxis


@pytest.mark.databricks_only
def test_find_all_taxis():
    results = taxis.find_all_taxis()
    assert results.count() > 5


@pytest.mark.local_only
def test_count_taxis(load_fixture):
    df = load_fixture("taxis_sample.json")
    assert taxis.count_taxis(df) == 3


@pytest.mark.local_only
def test_filter_taxis_by_vendor_id(load_fixture):
    df = load_fixture("taxis_sample.json")
    filtered = taxis.filter_taxis_by_vendor_id(df, "VTS")
    assert filtered.count() == 2
