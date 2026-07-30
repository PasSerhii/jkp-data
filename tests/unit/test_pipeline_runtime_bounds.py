"""Pipeline-level regression tests for source and runtime date propagation."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pytest

import jkp.data.main as pipeline
from jkp.data.config import DAILY_DOWNLOAD_WORKERS, ROLLING_INPUT_YEARS


def test_full_history_uses_the_accounting_start_date_bound(monkeypatch, tmp_path) -> None:
    """--full-history restores the pre-rolling-window behaviour for re-seeds."""
    download = MagicMock()
    monkeypatch.setattr(pipeline, "download_raw_data_tables", download)
    monkeypatch.setattr(pipeline, "setup_folder_structure", MagicMock())
    monkeypatch.setattr(
        pipeline,
        "gen_raw_data_dfs",
        MagicMock(side_effect=RuntimeError("stop after download")),
    )
    monkeypatch.setattr(
        pipeline,
        "get_xpressfeed_connection_info",
        MagicMock(return_value="postgresql://private-rds"),
    )

    try:
        pipeline.run_pipeline(
            output_dir=tmp_path,
            bypass_crsp=True,
            production_output=False,
            compustat_source="xpressfeed",
            full_history=True,
        )
    except RuntimeError as error:
        assert str(error) == "stop after download"
    else:
        raise AssertionError("pipeline should have stopped after the download step")

    assert download.call_args.kwargs["start_date"] == date(1949, 12, 31)


def test_default_source_bound_is_the_rolling_window(monkeypatch, tmp_path) -> None:
    """Without a flag a run downloads ROLLING_INPUT_YEARS back, not all history."""
    download = MagicMock()
    monkeypatch.setattr(pipeline, "download_raw_data_tables", download)
    monkeypatch.setattr(pipeline, "setup_folder_structure", MagicMock())
    monkeypatch.setattr(
        pipeline,
        "gen_raw_data_dfs",
        MagicMock(side_effect=RuntimeError("stop after download")),
    )
    monkeypatch.setattr(
        pipeline,
        "get_xpressfeed_connection_info",
        MagicMock(return_value="postgresql://private-rds"),
    )

    end = date(2026, 6, 30)
    try:
        pipeline.run_pipeline(
            output_dir=tmp_path,
            bypass_crsp=True,
            production_output=False,
            compustat_source="xpressfeed",
            end_date=end,
        )
    except RuntimeError as error:
        assert str(error) == "stop after download"
    else:
        raise AssertionError("pipeline should have stopped after the download step")

    assert download.call_args.kwargs["start_date"] == date(
        end.year - ROLLING_INPUT_YEARS, end.month, 1
    )


def test_full_history_and_start_date_together_are_rejected(tmp_path) -> None:
    """Two different lower bounds is a mistake, not something to silently resolve."""
    with pytest.raises(ValueError, match="only one"):
        pipeline.run_pipeline(
            output_dir=tmp_path,
            bypass_crsp=True,
            production_output=False,
            compustat_source="xpressfeed",
            start_date=date(2000, 1, 1),
            full_history=True,
        )


def test_reuse_raw_validates_inputs_and_skips_download(monkeypatch, tmp_path) -> None:
    download = MagicMock()
    validate = MagicMock()
    monkeypatch.setattr(pipeline, "download_raw_data_tables", download)
    monkeypatch.setattr(pipeline, "validate_reusable_raw_data", validate)
    monkeypatch.setattr(pipeline, "setup_folder_structure", MagicMock())
    monkeypatch.setattr(
        pipeline,
        "gen_raw_data_dfs",
        MagicMock(side_effect=RuntimeError("stop after raw validation")),
    )
    monkeypatch.setattr(
        pipeline,
        "get_xpressfeed_connection_info",
        MagicMock(return_value="postgresql://private-rds"),
    )

    try:
        pipeline.run_pipeline(
            output_dir=tmp_path,
            bypass_crsp=True,
            production_output=False,
            compustat_source="xpressfeed",
            reuse_raw=True,
        )
    except RuntimeError as error:
        assert str(error) == "stop after raw validation"
    else:
        raise AssertionError("pipeline should have stopped after raw validation")

    validate.assert_called_once_with(
        pipeline.DataPaths(base_dir=tmp_path.resolve()), bypass_crsp=True
    )
    download.assert_not_called()


def test_xpressfeed_pipeline_propagates_runtime_bounds(monkeypatch, tmp_path) -> None:
    step_names = (
        "setup_folder_structure",
        "download_raw_data_tables",
        "gen_raw_data_dfs",
        "prepare_comp_sf",
        "combine_crsp_comp_sf",
        "comp_industry",
        "merge_industry_to_world_msf",
        "ff_ind_class",
        "nyse_size_cutoffs",
        "classify_stocks_size_groups",
        "return_cutoffs",
        "add_ret_exc_wins",
        "market_returns",
        "standardized_accounting_data",
        "create_acc_chars",
        "combine_ann_qtr_chars",
        "market_chars_monthly",
        "create_world_data_prelim",
        "ap_factors",
        "firm_age",
        "mispricing_factors",
        "market_beta",
        "residual_momentum",
        "bidask_hl",
        "prepare_daily",
        "roll_apply_daily",
        "merge_roll_apply_daily_results",
        "finish_daily_chars",
        "merge_world_data_prelim",
        "quality_minus_junk",
        "merge_qmj_to_world_data",
        "filter_dsf",
        "filter_msf",
        "filter_world",
        "save_main_data",
        "save_daily_ret",
        "save_monthly_ret",
        "save_accounting_data",
        "save_output_files",
        "save_full_files_and_cleanup",
    )
    mocks: dict[str, MagicMock] = {}
    for name in step_names:
        mock = MagicMock(name=name)
        monkeypatch.setattr(pipeline, name, mock)
        mocks[name] = mock
    monkeypatch.setattr(
        pipeline,
        "get_xpressfeed_connection_info",
        MagicMock(return_value="postgresql://private-rds"),
    )
    wrds = MagicMock()
    monkeypatch.setattr(pipeline, "get_wrds_credentials", wrds)

    runtime_start = date(2000, 1, 1)
    runtime_end = date(2026, 7, 31)
    pipeline.run_pipeline(
        output_dir=tmp_path,
        bypass_crsp=True,
        production_output=False,
        start_date=runtime_start,
        end_date=runtime_end,
        compustat_source="xpressfeed",
    )

    wrds.assert_not_called()
    download_kwargs = mocks["download_raw_data_tables"].call_args.kwargs
    assert download_kwargs["start_date"] == runtime_start
    assert download_kwargs["end_date"] == runtime_end
    assert download_kwargs["raw_schema"] == "public"
    assert download_kwargs["connection_info"] == "postgresql://private-rds"
    assert download_kwargs["daily_download_workers"] == DAILY_DOWNLOAD_WORKERS
    assert mocks["standardized_accounting_data"].call_args.args[-1] == runtime_start

    assert mocks["comp_industry"].call_args.kwargs["end_date"] == runtime_end
    assert mocks["firm_age"].call_args.kwargs["bypass_crsp"] is True
    assert mocks["market_beta"].call_args.kwargs["end_date"] == runtime_end
    assert all(
        call.kwargs["end_date"] == runtime_end for call in mocks["residual_momentum"].call_args_list
    )
    assert all(
        call.kwargs["end_date"] == runtime_end for call in mocks["roll_apply_daily"].call_args_list
    )
    assert mocks["merge_roll_apply_daily_results"].call_args.kwargs["end_date"] == runtime_end
