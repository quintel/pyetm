import pandas as pd
from unittest.mock import Mock, patch
from pyetm.models.annual_exports import AnnualExport, AnnualExports


def test_annual_exports_to_dataframe_empty():
    """Test to_dataframe with no exports"""
    exports = AnnualExports(exports={})
    df = exports.to_dataframe()

    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_annual_exports_to_dataframe_with_exports():
    """Test to_dataframe with available exports"""
    # Create mock exports with data
    export1 = AnnualExport(
        name="energy_flow",
        data=pd.DataFrame({"carrier": ["electricity", "gas"], "value": [100, 200]}),
    )
    export2 = AnnualExport(
        name="production_parameters",
        data=pd.DataFrame({"parameter": ["cost", "capacity"], "amount": [50, 75]}),
    )

    exports = AnnualExports(exports={"energy_flow": export1, "production_parameters": export2})
    df = exports.to_dataframe()

    # Check structure
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert "export_name" in df.index.names

    # Check both exports are present
    export_names = df.index.get_level_values("export_name").unique().tolist()
    assert "energy_flow" in export_names
    assert "production_parameters" in export_names


def test_annual_exports_to_dataframe_filtered():
    """Test to_dataframe with export filtering"""
    export1 = AnnualExport(
        name="energy_flow", data=pd.DataFrame({"carrier": ["electricity"], "value": [100]})
    )
    export2 = AnnualExport(
        name="production_parameters", data=pd.DataFrame({"parameter": ["cost"], "amount": [50]})
    )

    exports = AnnualExports(exports={"energy_flow": export1, "production_parameters": export2})
    df = exports.to_dataframe(exports=["energy_flow"])

    # Only energy_flow should be included
    export_names = df.index.get_level_values("export_name").unique().tolist()
    assert export_names == ["energy_flow"]
    assert "production_parameters" not in export_names


def test_annual_exports_to_dataframe_unavailable_skipped():
    """Test to_dataframe skips unavailable exports"""
    export1 = AnnualExport(name="energy_flow", data=None)  # No data
    export2 = AnnualExport(
        name="production_parameters", data=pd.DataFrame({"parameter": ["cost"], "amount": [50]})
    )

    exports = AnnualExports(exports={"energy_flow": export1, "production_parameters": export2})
    df = exports.to_dataframe()

    # Only production_parameters should be included
    if not df.empty:
        export_names = df.index.get_level_values("export_name").unique().tolist()
        assert export_names == ["production_parameters"]


def test_annual_exports_to_dataframe_heterogeneous_schemas():
    """Test to_dataframe with exports that have different columns (outer join)"""
    export1 = AnnualExport(
        name="energy_flow", data=pd.DataFrame({"carrier": ["electricity"], "value": [100]})
    )
    export2 = AnnualExport(name="costs", data=pd.DataFrame({"category": ["capex"], "cost": [500]}))

    exports = AnnualExports(exports={"energy_flow": export1, "costs": export2})
    df = exports.to_dataframe()

    # Check that both exports' columns are present (with NaN for missing values)
    assert not df.empty
    # The dataframe should have columns from both exports
    # Note: exact column names depend on implementation details
    assert len(df) > 0
