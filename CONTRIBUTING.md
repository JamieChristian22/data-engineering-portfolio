# Contributing to the Data Engineering Portfolio

Thank you for considering improvements to this independent data engineering portfolio. Contributions should improve documentation, reproducibility, data quality, or engineering practices without overstating the results of local demonstrations.

## Before you contribute

- Review the [README](README.md), [Code of Conduct](CODE_OF_CONDUCT.md), and [Security Policy](SECURITY.md).
- Use public or synthetic sample datasets only, unless you have explicit permission to share the data.
- Do not commit credentials, database connection strings, API tokens, personal information, or proprietary data.
- Clearly label simulated pipelines and distinguish design examples from verified production deployments.

## Helpful contributions

- Fix documentation, setup steps, and broken links.
- Add reproducible validation tests for ingestion, transformation, and analytics models.
- Improve SQL, dbt, Python, and orchestration examples with clear explanations.
- Add data quality checks for schema consistency, null values, uniqueness, and incremental-loading behavior.
- Improve sample pipeline logging, error handling, and safe configuration defaults.

## Contribution workflow

1. Check existing issues and create a short issue for larger changes.
2. Work on a descriptive feature branch.
3. Keep changes focused and avoid altering unrelated project artifacts.
4. Follow the specific project's README for environment setup and verification.
5. Where applicable, install dependencies and run the documented demo:

   ```bash
   pip install -r requirements.txt
   bash run_demo_all.sh
   ```

6. Run any relevant project-specific tests, and describe the tests you actually executed.
7. Submit a pull request outlining the change, expected impact, test results, and any limitations.

A documentation or configuration change should not claim a successful execution unless a reproducible check has actually been performed.

## Responsible reporting

Do not disclose sensitive vulnerabilities or credentials through a public issue. Follow [SECURITY.md](SECURITY.md) for private reporting instructions.
