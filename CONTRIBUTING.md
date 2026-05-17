# Contributing

This is a small personal project. Contributions are welcome but optional. There is no formal release process and no SLA on responses.

## Reporting issues

If you find:

- A region lookup that disagrees with the relevant DNO's published MPAN-region map or with a known correct case
- A tariff-set recommendation that omits or wrongly includes a charge line for a given site archetype
- An out-of-date band threshold or zone name
- A broken link or stale reference document

please open a GitHub Issue with:

1. A minimal reproducible case (lat/lon, site archetype, capacity)
2. The expected output
3. The observed output
4. A pointer to the authoritative source (NESO publication, DNO methodology, CDCM section, etc.)

## Pull requests

- Open a branch off `master`.
- Keep changes scoped — one logical change per PR.
- For visual or layout-only changes, include a before/after screenshot or a description of the affected screen states.
- For methodology or data changes (zone polygons, band thresholds, tariff-set logic), link the authoritative source in the PR description.
- Do not commit the full-resolution `dno_*.geojson` or `gsp_*.geojson` files — only the `slim_*` variants are tracked. See `.gitignore`.

## Local development

The site is static and runs without a build step. To work on it:

```
python -m http.server 8000
# then open http://localhost:8000
```

The Python lookup script:

```
python -m venv .venv
.venv\Scripts\activate     # Windows
source .venv/bin/activate  # macOS / Linux
pip install pyproj openpyxl
python tnuos_duos_lookup.py --lat 52.2053 --lon 0.1218
```

## Data updates

When NESO publishes new boundary files or a new tariff year:

1. Drop the full-resolution GeoJSON into `data/` (it stays gitignored).
2. Regenerate the `slim_*` variant used by the web app.
3. Update the date suffix on the slim filename and the references in `README.md` and `DISCLAIMER.md`.
4. Update tariff zone names, demand-zone mappings, and band thresholds in `index.html` and `tnuos_duos_lookup.py` to match the new publication.

## Scope guardrails

This tool is intentionally a lookup and orientation aid. It is not the right place for:

- A full network-charge calculator (multiplies tariffs by volumes)
- A bills-validation tool
- A connection-cost or capacity-market modeller

If a change would push the tool past "tell me which lines apply and roughly which band", consider whether it belongs in a separate project instead.
