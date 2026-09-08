# Global Factor, Stock, and Firm data

This repo contains Python code to generate the global dataset of factor returns, stock returns, and firm characteristics from [“Is there a Replication Crisis in Finance?”](https://onlinelibrary.wiley.com/doi/full/10.1111/jofi.13249) by Jensen, Kelly, and Pedersen (Journal of Finance, 2023).

## Data Usage

This package requires a valid [WRDS](https://wrds-www.wharton.upenn.edu/) subscription. The authors do not distribute WRDS, CRSP, Compustat, or IBES data; this tool only orchestrates the user's own licensed downloads and transformations. Outputs generated locally are derived from your licensed WRDS data and remain subject to your WRDS and vendor license terms.

If you do not have a WRDS subscription, you can still access pre-computed factor portfolios at [jkpfactors.com](https://jkpfactors.com) and pre-computed stock returns and firm characteristics at the [WRDS Global Factor Data page](https://wrds-www.wharton.upenn.edu/pages/get-data/contributed-data-forms/global-factor-data/).

## Instructions

### Prerequisites

- For the production Compustat-only build, put the licensed XpressFeed RDS URL
  in `COMPUSTAT` in the repository `.env` file. SQLAlchemy URLs such as
  `postgresql+psycopg2://...` are accepted and normalized automatically.
- WRDS credentials are needed only for `--compustat-source wrds` comparison
  runs or for workflows that do not bypass CRSP.
- Ensure you have [uv](https://docs.astral.sh/uv/getting-started/installation/#standalone-installer) installed on your system.

### Steps

1. **Clone the repo**

   - Clone the folder to your local machine by running the following command from your terminal:
     ```sh
     git clone https://github.com/bkelly-lab/jkp-data.git
     ```
2. **Configure the data source**

   The default build reads the WRDS-compatible `comp.*` views and
   `ff.factors_monthly` from XpressFeed RDS. The `.env` file is read without
   exporting its values into the process environment, and connection secrets
   are not logged.

   ```sh
   jkp build data/ --compustat-source xpressfeed --bypass-crsp
   ```

   `--start-date` is a calculation-history bound, not just an output filter.
   It defaults to the single `config.ACCOUNTING_START_DATE` value
   (`1949-12-31`), matching the original accounting-history floor and retaining
   all history currently available in the downloaded time-series sources. Firm
   age can begin before that floor, so each download also creates a compact
   `comp_age_anchor.parquet` from indexed full-history minima.

   `comp.secd` and `comp.g_secd` are downloaded in deterministic 250-security
   parts by a shared two-worker pool. Each worker owns its database connection,
   and interrupted runs reuse parts whose Parquet data and manifest still match
   the requested pairs, columns, and date bounds. Use
   `--daily-download-workers 1` for a serial comparison; values above 4 are
   rejected. A failed batch is retried up to three times with a fresh connection
   and 5, 10, then 20 seconds of backoff. Keep the production default at 2 until
   database monitoring shows that a higher setting is safe.

   Download timing and throughput are written to
   `run_logs/<run-id>/download_telemetry.csv`, including table/batch rows,
   compressed bytes, rows/second, MiB/second, retries, timeouts, worker number,
   and cumulative completion percentages.

   Builds preserve the WRDS/SAS source cells exactly. Exchange
   eligibility is resolved from `sec_history.EXCHG` for each observation date,
   and accounting observations cannot enter a month before their reported
   `pdate`/`fdate`/`rdq`. Each run writes `source_snapshot_manifest.json` with
   the exact Fama-French input hash, date range, and latest risk-free rate so RF
   snapshot differences can be audited.

   By default, the end date is calculated once at process startup as the final
   calendar day of the previous month. A command-line `--end-date` overrides
   that default and is propagated through downloads, industry histories,
   rolling monthly/daily calculations, and production exports. If portfolio
   outputs are generated separately, pass the same date there:

   ```sh
   jkp portfolio data/ --end-date 2026-07-31
   ```

   To run a deliberate WRDS regression comparison instead, configure WRDS
   credentials and select it explicitly:

   - To save your WRDS credentials, navigate to the `jkp-data/` folder and run:
     ```sh
     jkp connect
     ```
     Kindly follow the prompts.

     Note: If you need to change your password or credentials, run `jkp connect --reset` and then `jkp connect`

   - **Credential precedence.** When the pipeline needs WRDS credentials, it
     resolves them from the `WRDS_USERNAME`/`WRDS_PASSWORD` environment
     variables, then the system keyring, then a libpq password file
     (`$PGPASSFILE`, else `~/.pgpass`). Run `jkp connect --help` for details.

   On a Slurm/HPC compute node, run `jkp connect` once on the login node — it
   has no system keyring but does have a terminal, so it writes `~/.pgpass`
   (mode 600). Because `$HOME` is shared with the compute nodes, the batch job
   reads it via libpq with no further setup; you do not run `jkp connect`
   inside the job.

3. **Run the script**

   - We run the code via a Slurm scheduler, but we also show how to run it in an interactive Python session.

   - Before running the following commands, make sure you are in `jkp-data/`

   - On a cluster with a Slurm scheduler, run:
     ```sh
     sbatch slurm/submit_job_som_hpc.slurm
     ```
     to create the factor returns, stock returns, and firm characteristics.
     The script writes to `data/` by default. To use a different output directory, pass
     `--output-dir`: `sbatch slurm/submit_job_som_hpc.slurm --output-dir /path/to/output`.
     Relative paths resolve against the directory you submit from, and the resolved
     destination is echoed in the job log. The script rejects unrecognized options and
     stray arguments, so a mistyped flag or an unquoted path containing a space fails
     immediately instead of writing to the wrong place. It parses options with the
     enhanced (util-linux) `getopt`, which is present by default on mainstream Linux
     distributions but is not a dependency of the `jkp` package itself.
     Note that the batch script passes `--force` to `jkp build`, so it overwrites an
     existing output directory without prompting (an interactive `jkp build` asks first).

     In an interactive session, run:
     ```sh
     jkp build data/ --compustat-source xpressfeed --bypass-crsp
     ```
     to create the stock returns and firm characteristics, and
     ```sh
     jkp portfolio data/
     ```
     to create the factor returns.

   **IMPORTANT:** When starting the code, you may be prompted to grant access to WRDS using two-factor authentication, for example via a Duo notification. You need to approve this request, as the program will otherwise fail. After a few seconds or minutes, you should see data being created in the output directory. If that is not the case, please check your internet connection or credentials.

When the code is finished, you can find the output in the `processed/` subdirectory of your output directory (e.g. `data/processed/`).
Please see the release notes (`documentation/release_notes.html`) for a description of the output files and a comparison between the output of the SAS/R codebase and the new Python codebase.

### Docker and AWS

To run the monthly production build on AWS — which script to use, what each
parameter does, and where the credentials come from — see
[OPERATIONS.md](OPERATIONS.md).

For a reproducible container build, mounted-output layout, and Amazon ECR/EC2
commands, see [DOCKER.md](DOCKER.md). Credentials and licensed/generated data
are excluded from the image and Docker build context.

## Notes
- By default, output files are written in Parquet format. To output CSV files instead (with quoted strings to preserve leading zeros in identifiers like `gvkey`), run:
  ```sh
  jkp portfolio data/ --output-format csv
  ```

- By default, the data ends on the final calendar day of the previous month,
  calculated when the process starts. Use `--end-date YYYY-MM-DD` to reproduce
  a deliberate historical cutoff.

- **Persistent WRDS Connection**: If you're running on an HPC cluster with NAT IP rotation (such as Yale's Bouchet cluster), you may receive many MFA prompts during data download. This happens because each database query creates a new TCP connection, and the NAT gateway assigns a random outbound IP to each connection. WRDS sees these as connections from different locations and triggers MFA for each.

  To avoid this, use the `--persistent-connection` flag, which maintains a single database connection throughout the download process:
  ```sh
  # Interactive session
  jkp build data/ --persistent-connection

  # Slurm job (set environment variable)
  sbatch --export=ALL,PERSISTENT_WRDS_CONNECTION=1 slurm/submit_job_som_hpc.slurm

  # Slurm job with a custom output directory
  sbatch --export=ALL,PERSISTENT_WRDS_CONNECTION=1 slurm/submit_job_som_hpc.slurm --output-dir /path/to/output
  ```
  This reduces MFA prompts from ~26 (one per table) to just 1 (at connection time).

- To run the code, we utilize a high performance computing cluster, where we request 450 GB RAM and 128 CPU cores. Running the routine takes about 6 hours.

- To understand the data, please refer to our [documentation](https://jkpfactors.s3.amazonaws.com/documents/Documentation.pdf).

- For a history of changes to the underlying data, see [CHANGELOG_DATA.md](CHANGELOG_DATA.md).

- We distribute the global factor returns generated from this codebase at [jkpfactors.com](https://jkpfactors.com) and the stock returns and firm characteristics at [wrds-www.wharton.upenn.edu/pages/get-data/contributed-data-forms/global-factor-data/](https://wrds-www.wharton.upenn.edu/pages/get-data/contributed-data-forms/global-factor-data/).

- The original SAS/R codebase is still available at [github.com/bkelly-lab/ReplicationCrisis](https://github.com/bkelly-lab/ReplicationCrisis), but we recommend using this new Python codebase for future work.

## License

Code in this repository is released under the [MIT License](LICENSE).

Data distributed in this repository is licensed under [Creative Commons Attribution-NonCommercial 4.0 (CC BY-NC 4.0)](DATA_LICENSE).

See [LICENSE](LICENSE) and [DATA_LICENSE](DATA_LICENSE) for details.
