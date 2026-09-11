"""
Shared helpers for notebook source-text assertions.

These read notebook source text from the modular layout; they never execute a
notebook and never require Spark.
"""

import os

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
NOTEBOOKS = os.path.join(ROOT, "notebooks")
SHARED = os.path.join(NOTEBOOKS, "shared")
SOURCES = os.path.join(NOTEBOOKS, "sources")

SOURCE_TOKENS = ("oracle", "sqlserver")

# Every notebook a source folder must provide.
REQUIRED_SOURCE_NOTEBOOKS = (
    "NB00A_UpsertAndValidateConnection.py",
    "NB01_SourceInventory.py",
    "NB01A_SourceAssessment.py",
    "NB13_SQLObjectAssessmentAndConversion.py",
    "TEST_CONNECTION.py",
)

SHARED_NOTEBOOKS = (
    "NB00_ControlTableInit.py",
    "NB01B_RegisterSelectedTables.py",
    "NB02_TypeNormalization.py",
    "NB03_MappingRulesGeneration.py",
    "NB04_MappingValidation.py",
    "NB07_TableDecisionGeneration.py",
    "NB08_TargetProvisioning.py",
    "NB09_FullLoad.py",
    "NB10_PostFullLoadState.py",
    "NB11a_DeltaSyncPrep.py",
    "NB11b_DeltaSyncApply.py",
    "NB12_ValidationAndReconciliation.py",
    "NB14_RetryFailedTables.py",
    "NB15_BronzeToSilverETL.py",
    "NB16_NotifyFailures.py",
    "NB17_DashboardViews.py",
)


def _read(path):
    with open(path, "r", encoding="utf-8") as fh:
        return fh.read()


def shared_nb(name):
    return _read(os.path.join(SHARED, name))


def source_nb(source, name):
    return _read(os.path.join(SOURCES, source, name))


def source_nb_path(source, name):
    return os.path.join(SOURCES, source, name)


def all_shared_notebooks():
    """Shared notebooks plus shared/_common.py."""
    return SHARED_NOTEBOOKS + ("_common.py",)


def all_source_notebooks():
    """(source_token, filename) for every source notebook that exists."""
    out = []
    for token in SOURCE_TOKENS:
        folder = os.path.join(SOURCES, token)
        if not os.path.isdir(folder):
            continue
        for name in sorted(os.listdir(folder)):
            if name.endswith(".py"):
                out.append((token, name))
    return out
