"""Module containing a Valida schema for the workflow spec file."""

from valida import Schema, Rule, Value, Key, Index
from valida.datapath import ListValue, MapValue

# TODO: write this schema in yaml

_RULES = []

SPEC_SCHEMA = Schema(_RULES)
