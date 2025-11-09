# Hangarbay examples

Example notebooks demonstrating the Hangarbay Python API.

## Notebooks

### [LAPD fleet analysis](lapd_fleet_analysis.ipynb)

A comprehensive example showing how to:
- Load FAA aircraft registry data
- Search for fleets using wildcard patterns (`LAPD|Los Angeles Police`)
- Analyze aircraft by manufacturer, year and status
- Look up individual aircraft details
- Export data for further analysis

**Use case**: Analyzing municipal government aircraft fleets (police, fire departments)

## Running the notebooks

1. **Install hangarbay:**
   ```bash
   pip install hangarbay  # (coming soon to PyPI)
   # or for development:
   pip install -e ".[dev]"
   ```

2. **Install Jupyter:**
   ```bash
   pip install jupyter matplotlib seaborn
   ```

3. **Launch Jupyter:**
   ```bash
   jupyter notebook
   ```

4. **Open a notebook** and run the cells!

## Data location

All notebooks use data stored in `~/.hangarbay/data/` by default. The first time you run `hb.load_data()`, it will download ~400MB of FAA data. This is a one-time operation, and the data will be available to all notebooks.

## Need help?

- Check the [main README](../README.md) for full documentation
- See the [API documentation](../hangarbay/api.py) for all available functions
- Open an issue on GitHub if you have questions

