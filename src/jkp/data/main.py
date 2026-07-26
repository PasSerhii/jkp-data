from datetime import date
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
    APPLY_COMPUSTAT_CORRECTIONS,
    BYPASS_CRSP,
    PRODUCTION_OUTPUT,
    ROLLING_DAILY_SPECS,
)
from .config import (
    END_DATE as DEFAULT_END_DATE,
)
from .database_sources import CompustatSource, get_xpressfeed_connection_info
from .paths import DataPaths
from .production import export_production
from .runtime_monitor import get_active_monitor, monitor_pipeline
from .wrds_credentials import get_wrds_credentials


@monitor_pipeline
def run_pipeline(
    *,
    persistent_connection: bool = False,
    output_dir: Path,
    bypass_crsp: bool = BYPASS_CRSP,
    production_output: bool = PRODUCTION_OUTPUT,
    apply_compustat_corrections: bool = APPLY_COMPUSTAT_CORRECTIONS,
    start_date: date | None = None,
    end_date: date | None = None,
    compustat_source: CompustatSource | str = CompustatSource.xpressfeed,
    metrics_interval_seconds: float = 60.0,
    reuse_raw: bool = False,
    daily_download_workers: int = 2,
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
    effective_start_date = DEFAULT_ACCOUNTING_START_DATE if start_date is None else start_date
    effective_end_date = DEFAULT_END_DATE if end_date is None else end_date
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
        apply_compustat_corrections=apply_compustat_corrections,
        compustat_source=source.value,
        persistent_connection=persistent_connection,
        metrics_interval_seconds=metrics_interval_seconds,
        reuse_raw=reuse_raw,
        daily_download_workers=daily_download_workers,
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
    prepare_comp_sf(
        paths,
        "both",
        bypass_crsp=bypass_crsp,
        apply_correction=apply_compustat_corrections,
    )
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
    for sfx, min_obs, vars_ in ROLLING_DAILY_SPECS:
        for var in vars_:
            roll_apply_daily(paths, var, sfx, min_obs, end_date=effective_end_date)
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
    save_full_files_and_cleanup(paths, clear_interim=True)
