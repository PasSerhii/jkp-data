import contextvars
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from functools import partial
from pathlib import Path

from .aux_functions import (
    acc_chars_list,
    add_ret_exc_wins,
    ap_factors,
    bidask_hl,
    classify_stocks_size_groups,
    combine_ann_qtr_chars,
    combine_crsp_comp_sf,
    comp_industry,
    create_acc_chars,
    create_world_data_prelim,
    crsp_industry,
    download_raw_data_tables,
    ff_ind_class,
    filter_dsf,
    filter_msf,
    filter_world,
    finish_daily_chars,
    firm_age,
    gen_raw_data_dfs,
    market_beta,
    market_chars_monthly,
    market_returns,
    merge_industry_to_world_msf,
    merge_qmj_to_world_data,
    merge_roll_apply_daily_results,
    merge_world_data_prelim,
    mispricing_factors,
    nyse_size_cutoffs,
    prepare_comp_sf,
    prepare_crsp_sf,
    prepare_daily,
    quality_minus_junk,
    residual_momentum,
    return_cutoffs,
    roll_apply_daily,
    save_accounting_data,
    save_daily_ret,
    save_full_files_and_cleanup,
    save_main_data,
    save_monthly_ret,
    save_output_files,
    setup_folder_structure,
    standardized_accounting_data,
    validate_reusable_raw_data,
)
from .config import (
    ACCOUNTING_START_DATE as DEFAULT_ACCOUNTING_START_DATE,
)
from .config import (
    BYPASS_CRSP,
    DAILY_DOWNLOAD_WORKERS,
    MAX_LOOKBACK_MONTHS,
    PRODUCTION_OUTPUT,
    ROLLING_DAILY_SPECS,
    ROLLING_DAILY_WORKERS,
    ROLLING_INPUT_YEARS,
)
from .config import (
    END_DATE as DEFAULT_END_DATE,
)
from .database_sources import CompustatSource, get_xpressfeed_connection_info
from .paths import DataPaths
from .production import export_production
from .runtime_monitor import get_active_monitor, monitor_pipeline
from .wrds_credentials import get_wrds_credentials


def _rolling_start_date(end: date) -> date:
    """Earliest source date a run needs to keep every characteristic valid at ``end``.

    Counts ``config.ROLLING_INPUT_YEARS`` back from the end date so a monthly
    build carries a constant span rather than one that grows a year every year.
    Anchored to the first of the month, so the window is never a few days short
    of the full span.
    """
    return date(end.year - ROLLING_INPUT_YEARS, end.month, 1)


def _resolve_source_window(
    start_date: date | None, end_date: date, full_history: bool
) -> tuple[date | None, str]:
    """Pick the source download's lower bound, and a label naming which rule won.

    Three ways to bound a run, in precedence order: an explicit ``start_date``,
    ``full_history`` (``config.ACCOUNTING_START_DATE``, for re-seeding a
    downstream store or reissuing after a change that rewrites history), and
    otherwise the rolling window that keeps a monthly build's cost flat.

    Returns the bound (``None`` means unbounded) and a label recorded in
    ``run_summary.json``, so a run states which rule produced its window.
    """
    if full_history and start_date is not None:
        raise ValueError(
            "full_history and start_date set different source lower bounds; pass only one."
        )
    if start_date is not None:
        return start_date, "explicit"
    if full_history:
        return DEFAULT_ACCOUNTING_START_DATE, "full-history"
    return _rolling_start_date(end_date), f"rolling-{ROLLING_INPUT_YEARS}y"


def _roll_label(var: str, sfx: str, min_obs: int) -> str:
    """Step name for one rolling-daily job.

    The variable alone is not unique: ``zero_trades`` is computed over the 21d, 126d
    and 252d windows, so three jobs would otherwise share a label and collapse into
    one another in step_timings.csv.

    Fields are separated by ``;`` rather than ``,``: step names land in a CSV column,
    and the awk readers in scripts/ec2-benchmark split on every comma irrespective of
    quoting, which would shift every later field on these rows.
    """
    return f"roll_apply_daily[stat={var};window={sfx.lstrip('_')};min_obs={min_obs}]"


def _run_rolling_daily(paths: DataPaths, end_date: date) -> None:
    """Compute every rolling daily window, overlapping independent calculations.

    Each (window, variable) pair reads the same prepared daily panel and writes
    its own ``__roll{sfx}_{var}.parquet``, so the combinations neither share
    mutable state nor collide on output paths. Threads rather than processes:
    the work happens inside Polars, which releases the GIL, and passing
    LazyFrames across process boundaries would cost more than it saves.
    ``config.ROLLING_DAILY_WORKERS = 1`` restores sequential execution.
    """
    jobs = [(var, sfx, min_obs) for sfx, min_obs, vars_ in ROLLING_DAILY_SPECS for var in vars_]
    if ROLLING_DAILY_WORKERS <= 1 or len(jobs) <= 1:
        for var, sfx, min_obs in jobs:
            roll_apply_daily(
                paths,
                var,
                sfx,
                min_obs,
                end_date=end_date,
                _step_name=_roll_label(var, sfx, min_obs),
            )
        return

    monitor = get_active_monitor()
    # One parent step covering the fan-out. Children run concurrently, so their
    # durations sum to more than the parent's: only depth-0 rows may be summed.
    parent_token = monitor.step_started("rolling_daily_fanout") if monitor is not None else None
    try:
        with ThreadPoolExecutor(max_workers=ROLLING_DAILY_WORKERS) as pool:
            # A worker thread starts with an empty context, so the _ACTIVE_MONITOR
            # and step-stack ContextVars would both read as unset and every child
            # step would go unrecorded. Copying the context per submit carries the
            # monitor and the parent step into the worker.
            futures = [
                pool.submit(
                    contextvars.copy_context().run,
                    partial(
                        roll_apply_daily,
                        paths,
                        var,
                        sfx,
                        min_obs,
                        end_date=end_date,
                        _step_name=_roll_label(var, sfx, min_obs),
                    ),
                )
                for var, sfx, min_obs in jobs
            ]
            # Let every job settle before raising, so a failure cannot leave the
            # merge step reading a half-written set of windows.
            errors = [future.exception() for future in futures]
    except BaseException as error:
        if monitor is not None and parent_token is not None:
            monitor.step_finished(parent_token, error)
        raise
    first_error = next((item for item in errors if item is not None), None)
    if monitor is not None and parent_token is not None:
        monitor.step_finished(parent_token, first_error)
    if first_error is not None:
        raise first_error


@monitor_pipeline
def run_pipeline(
    *,
    persistent_connection: bool = False,
    output_dir: Path,
    bypass_crsp: bool = BYPASS_CRSP,
    production_output: bool = PRODUCTION_OUTPUT,
    start_date: date | None = None,
    end_date: date | None = None,
    compustat_source: CompustatSource | str = CompustatSource.xpressfeed,
    metrics_interval_seconds: float = 60.0,
    reuse_raw: bool = False,
    daily_download_workers: int = DAILY_DOWNLOAD_WORKERS,
    keep_interim: bool = False,
    full_history: bool = False,
) -> None:
    """Run the full JKP data generation pipeline.

    When ``bypass_crsp`` is True the pipeline is built from Compustat only:
    all CRSP downloads and CRSP-specific processing steps are skipped, mirroring
    the SAS ``bypass_crsp=1`` path. XpressFeed RDS is the default Compustat
    source and requires this mode because it does not contain CRSP. Select
    ``compustat_source="wrds"`` for historical comparison runs.
    """
    paths = DataPaths(base_dir=output_dir.resolve())
    source = CompustatSource(compustat_source)
    if source is CompustatSource.xpressfeed:
        if not bypass_crsp:
            raise ValueError(
                "The XpressFeed RDS contains Compustat and Fama-French data but not CRSP. "
                "Use --bypass-crsp, or select --compustat-source wrds for a CRSP build."
            )
        source_connection_info = get_xpressfeed_connection_info()
        source_label = "XpressFeed RDS"
        raw_schema = "public"
        username = None
        password = None
    else:
        creds = get_wrds_credentials()
        source_connection_info = None
        source_label = "WRDS"
        raw_schema = "comp"
        username = creds.username
        password = creds.password
    effective_end_date = DEFAULT_END_DATE if end_date is None else end_date
    effective_start_date, source_window = _resolve_source_window(
        start_date, effective_end_date, full_history
    )
    if (
        effective_start_date is not None
        and effective_end_date is not None
        and effective_start_date > effective_end_date
    ):
        raise ValueError(
            f"start_date ({effective_start_date}) must be on or before end_date ({effective_end_date})"
        )

    monitor = get_active_monitor()
    if monitor is None:  # pragma: no cover - run_pipeline always installs one
        raise RuntimeError("pipeline runtime monitor was not initialized")
    monitor.configure(
        start_date=effective_start_date,
        end_date=effective_end_date,
        bypass_crsp=bypass_crsp,
        production_output=production_output,
        compustat_source=source.value,
        persistent_connection=persistent_connection,
        metrics_interval_seconds=metrics_interval_seconds,
        reuse_raw=reuse_raw,
        daily_download_workers=daily_download_workers,
        keep_interim=keep_interim,
        source_window=source_window,
    )
    # A window shorter than the longest lookback nulls seas_16_20 everywhere,
    # silently -- the characteristic simply fails its own observation gate. The
    # rolling default always clears this; an explicit start_date may not. A None
    # bound means unbounded history, which cannot be too short.
    if effective_start_date is not None:
        window_months = (effective_end_date.year - effective_start_date.year) * 12 + (
            effective_end_date.month - effective_start_date.month
        )
        if window_months < MAX_LOOKBACK_MONTHS:
            monitor.note(
                f"WARNING source window is {window_months} months, short of the "
                f"{MAX_LOOKBACK_MONTHS} the longest lookback needs: seas_16_20an/na will "
                f"be null for every security. Omit start_date for the rolling "
                f"{ROLLING_INPUT_YEARS}-year default."
            )

    interim = paths.interim_dir

    monitor.set_phase("initialization")
    setup_folder_structure(paths)
    monitor.set_phase("source_download")
    if reuse_raw:
        monitor.note("Validating and reusing existing raw source downloads")
        validate_reusable_raw_data(paths, bypass_crsp=bypass_crsp)
    else:
        download_raw_data_tables(
            paths,
            username=username,
            password=password,
            connection_info=source_connection_info,
            source_label=source_label,
            raw_schema=raw_schema,
            end_date=effective_end_date,
            persistent_connection=persistent_connection,
            bypass_crsp=bypass_crsp,
            start_date=effective_start_date,
            daily_download_workers=daily_download_workers,
        )
    monitor.set_phase("security_panels")
    gen_raw_data_dfs(paths, bypass_crsp=bypass_crsp)
    prepare_comp_sf(paths, "both", bypass_crsp=bypass_crsp)
    if not bypass_crsp:
        prepare_crsp_sf(paths, "m")
        prepare_crsp_sf(paths, "d")
    combine_crsp_comp_sf(paths, bypass_crsp=bypass_crsp)
    if not bypass_crsp:
        crsp_industry(paths)
    monitor.set_phase("market_returns")
    comp_industry(paths, end_date=effective_end_date)
    merge_industry_to_world_msf(paths, bypass_crsp=bypass_crsp)
    ff_ind_class(paths, interim / "__msf_world2.parquet")
    nyse_size_cutoffs(paths, interim / "__msf_world3.parquet", bypass_crsp=bypass_crsp)
    classify_stocks_size_groups(paths)
    return_cutoffs(paths, "m", 0)
    return_cutoffs(paths, "d", 0)
    add_ret_exc_wins(paths, "m")
    add_ret_exc_wins(paths, "d")
    market_returns(
        paths,
        interim / "world_dsf.parquet",
        "d",
        1,
        interim / "return_cutoffs_daily.parquet",
        interim / "nyse_cutoffs.parquet",
    )
    market_returns(
        paths,
        interim / "world_msf.parquet",
        "m",
        1,
        interim / "return_cutoffs.parquet",
        interim / "nyse_cutoffs.parquet",
    )
    monitor.set_phase("accounting_characteristics")
    standardized_accounting_data(
        paths, "world", 1, interim / "world_msf.parquet", 1, effective_start_date
    )
    create_acc_chars(
        paths,
        interim / "acc_std_ann.parquet",
        interim / "achars_world.parquet",
        4,
        18,
        acc_chars_list(),
        interim / "world_msf.parquet",
        "",
    )
    create_acc_chars(
        paths,
        interim / "acc_std_qtr.parquet",
        interim / "qchars_world.parquet",
        4,
        18,
        acc_chars_list(),
        interim / "world_msf.parquet",
        "_qitem",
    )
    combine_ann_qtr_chars(
        paths,
        interim / "achars_world.parquet",
        interim / "qchars_world.parquet",
        acc_chars_list(),
        "_qitem",
    )
    market_chars_monthly(paths, interim / "world_msf.parquet", interim / "market_returns.parquet")
    create_world_data_prelim(
        paths,
        interim / "world_msf.parquet",
        interim / "market_chars_m.parquet",
        interim / "acc_chars_world.parquet",
        interim / "world_data_prelim.parquet",
    )
    monitor.set_phase("factor_models")
    ap_factors(
        paths,
        interim / "ap_factors_daily.parquet",
        "d",
        interim / "world_dsf.parquet",
        interim / "world_data_prelim.parquet",
        interim / "market_returns_daily.parquet",
        10,
        3,
    )
    ap_factors(
        paths,
        interim / "ap_factors_monthly.parquet",
        "m",
        interim / "world_msf.parquet",
        interim / "world_data_prelim.parquet",
        interim / "market_returns.parquet",
        10,
        3,
    )
    firm_age(paths, interim / "world_msf.parquet", bypass_crsp=bypass_crsp)
    mispricing_factors(paths, interim / "world_data_prelim.parquet", 10, min_fcts=3)
    market_beta(
        paths,
        interim / "beta_60m.parquet",
        interim / "world_msf.parquet",
        interim / "ap_factors_monthly.parquet",
        60,
        36,
        end_date=effective_end_date,
    )
    residual_momentum(
        paths,
        "resmom_ff3",
        interim / "world_msf.parquet",
        interim / "ap_factors_monthly.parquet",
        36,
        24,
        12,
        1,
        end_date=effective_end_date,
    )
    residual_momentum(
        paths,
        "resmom_ff3",
        interim / "world_msf.parquet",
        interim / "ap_factors_monthly.parquet",
        36,
        24,
        6,
        1,
        end_date=effective_end_date,
    )
    monitor.set_phase("daily_characteristics")
    bidask_hl(
        paths,
        interim / "corwin_schultz.parquet",
        interim / "world_dsf.parquet",
        interim / "market_returns_daily.parquet",
        10,
    )
    prepare_daily(paths, interim / "world_dsf.parquet", interim / "ap_factors_daily.parquet")
    _run_rolling_daily(paths, effective_end_date)
    merge_roll_apply_daily_results(paths, end_date=effective_end_date)
    finish_daily_chars(paths, interim / "market_chars_d.parquet")
    monitor.set_phase("final_outputs")
    merge_world_data_prelim(paths)
    quality_minus_junk(paths, interim / "world_data_-1.parquet", 10)
    merge_qmj_to_world_data(paths)
    filter_dsf(paths)
    filter_msf(paths)
    filter_world(paths)
    save_main_data(paths)
    save_daily_ret(paths)
    save_monthly_ret(paths)
    save_accounting_data(paths)
    save_output_files(paths)
    # Production CSVs must run before cleanup: they read the filtered interim
    # outputs and the raw SECD/G_SECD identifier tables, all cleared below.
    if production_output:
        export_production(paths, end_date=effective_end_date)
    save_full_files_and_cleanup(paths, clear_interim=not keep_interim)
