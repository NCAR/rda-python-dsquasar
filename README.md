# rda_python_dsquasar

RDA Python package to back up and recover RDA data archives to and from the
GLOBUS Quasar backup server.

## Programs

The package installs the following command-line utilities, all running as the
current user:

- `dsquasar` — back up and restore RDA dataset archives via Quasar
- `tacctar` — create tar bundles for Quasar archival
- `taccrec` — recover content from Quasar tar bundles

Run any program with `--help` (or `-h`) for full usage details.

## Environment setup

Create a Python environment first; package installs in the next section run
inside whichever environment you activate here.

### Option A — Python venv (DECS machines)

```bash
python3 -m venv $ENVHOME          # e.g. /glade/u/home/gdexdata/gdexmsenv
source $ENVHOME/bin/activate
```

### Option B — Conda (DAV/Casper)

```bash
conda create --prefix $ENVHOME python=3.12   # e.g. /glade/work/gdexdata/conda-envs/pg-gdex
conda activate $ENVHOME
```

## Installing rda-python-dsquasar

Pick whichever install mode fits your workflow.  All variants pull in the
transitive dependencies (`rda_python_common`, `rda_python_dsarch`)
automatically.

For local development, clone this repo alongside your project and install it
in editable mode so that changes are picked up without re-installing:

```bash
git clone https://github.com/NCAR/rda-python-dsquasar.git
cd rda-python-dsquasar
pip install -e .
```

To test a specific branch (e.g. an in-progress feature or fix branch), pass
`-b/--branch` to `git clone`:

```bash
git clone -b <branch-name> https://github.com/NCAR/rda-python-dsquasar.git
cd rda-python-dsquasar
pip install -e .
```

For a regular (non-editable) install from a checkout:

```bash
pip install /path/to/rda-python-dsquasar
```

For a production install on a system that uses the published distribution:

```bash
pip install rda_python_dsquasar
```
