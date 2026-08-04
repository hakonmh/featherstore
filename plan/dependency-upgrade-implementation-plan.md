# Dependency & Python upgrade implementation plan

## Goal

Support current Python and dependency versions while following existing project testing/CI patterns.

## Decisions

- `python_requires='>=3.11'` (pandas 3.x requires 3.11+; drop 3.8–3.10)
- Classifiers / docs / CI cover Python 3.11–3.14
- Dependency floors (no upper bounds):
  - `pandas>=2.2.0`
  - `polars[timezone]>=1.0.0`
  - `pyarrow>=14.0.0`
  - `numpy>=1.26.0` (dev/requirements)

## Assumptions

1. Breaking drop of Python <3.11 is acceptable for this release.
2. Ubuntu CI continues to pin **minimum** supported deps; macOS/Windows CI installs from `requirements.txt` (latest allowed).
3. Code/API fixes are limited to what the test suite needs for the new stack; no unrelated feature work.

## Todo

- [x] Update `setup.py`, `requirements.txt`, README, Overview, CHANGELOG, ReadTheDocs
- [x] Update GitHub Actions matrices
- [x] Install stack and run existing tests (red)
- [x] Fix compatibility issues until green
- [x] Run flake8 with project settings
- [x] Verify minimum deps (`pandas==2.2.0`, `polars==1.0.0`, `pyarrow==14.0.0`)
- [ ] Commit, push, open PR
