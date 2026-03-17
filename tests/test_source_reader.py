"""Tests for source_reader.py — validates SQL construction (no live JDBC needed)."""

import pytest

from src.readers.source_reader import _SOURCE_SQL


def test_source_sql_selects_emp_nbr():
    """Source SQL must alias TELE_EMP_NBR as EMP_NBR."""
    assert "TELE_EMP_NBR AS EMP_NBR" in _SOURCE_SQL


def test_source_sql_constructs_ph_nbr():
    """Source SQL must build PH_NBR via TRIM||TRIM||TRIM concatenation."""
    assert "TELE_EMGR_PH_AREA_CD" in _SOURCE_SQL
    assert "TELE_EMGR_PH_PREFIX" in _SOURCE_SQL
    assert "TELE_EMGR_PH_NUM" in _SOURCE_SQL
    assert "AS PH_NBR" in _SOURCE_SQL


def test_source_sql_includes_5_basic_phone_sets():
    """Source SQL includes all 5 BASIC phone set groups."""
    for i in range(1, 6):
        assert f"BASIC_PH_NBR_{i}" in _SOURCE_SQL


def test_source_sql_includes_home_away_priority_columns():
    """Source SQL includes HOME and AWAY priority sequence columns."""
    assert "TELE_HOME_PRI_FROM_1" in _SOURCE_SQL
    assert "TELE_AWAY_PRI_FROM_1" in _SOURCE_SQL
    assert "TELE_HOME_PRI_SEQ_1_1" in _SOURCE_SQL
    assert "TELE_AWAY_PRI_SEQ_1_1" in _SOURCE_SQL


def test_source_sql_has_from_clause():
    """Source SQL must have a FROM clause with the parameterised table."""
    assert "FROM {source_table}" in _SOURCE_SQL
