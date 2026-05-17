# TNUoS + DUoS Mapping

A single-page web app and companion Python script for looking up the **DUoS region, TNUoS demand zone, and TNUoS generation zone** of any site in Great Britain from a latitude/longitude, then suggesting the matching DUoS and TNUoS tariff line items to include in a commercial model.

Live site: https://joshuaelliott11.github.io/TNUOS-DUOS-Mapping/

## What it does

Network charges on a GB site sit across two regimes:

- **DUoS** (Distribution Use of System) — set by the 14 DNO licence areas and charged on distribution-connected sites.
- **TNUoS** (Transmission Network Use of System) — set by NESO across demand zones (for import) and generation zones (for export), and charged on both transmission-connected and embedded sites.

The exact tariff lines a site is liable for depend on (a) where the site sits on the map, and (b) what kind of site it is (pure demand, pure generation, storage, co-located, transmission-connected vs distribution-connected). This tool puts both together:

- **Region lookup**: ray-casts the supplied lat/lon against three GeoJSON layers — DNO licence areas (EPSG:27700), TNUoS demand GSP groups, and TNUoS generation zones — and returns the matching DUoS operator, demand zone, GSP code, and generation zone.
- **Tariff recommender**: picks one of ten site archetypes (distribution vs transmission, demand vs export vs storage vs co-located) and lists the DUoS and TNUoS line items to include, with the appropriate Site Specific / Residual / Triad / Embedded Export / Wider Generation labels and band suggestions based on supplied capacity or annual energy.

## How to use it

1. Open the live site (or `index.html` locally).
2. Enter latitude and longitude in decimal degrees (WGS84).
3. Select a site archetype.
4. Enter capacity (kVA / kW) or annual energy (kWh / MWh).
5. Click **Lookup Regions** for the region summary, or **Recommend Tariffs** for the full charge set.

The Python script `tnuos_duos_lookup.py` performs the same region lookup against the full-resolution NESO GeoJSON files (not the slim versions used by the web app) and supports single-point and batch (CSV / XLSX) lookups:

```
python tnuos_duos_lookup.py --lat 52.2053 --lon 0.1218
python tnuos_duos_lookup.py --input sites.csv --output sites_with_regions.csv
```

The script depends on `pyproj` and, for Excel mode, `openpyxl`.

## Data sources

| Layer | Source |
| --- | --- |
| DNO licence areas | NESO open data — DNO licence area boundaries (EPSG:27700) |
| GSP groups | NESO open data — GSP group polygons |
| TNUoS generation zones | NESO TNUoS generation zone polygons |
| 2026/27 TNUoS tariffs | NESO 2026/27 Final TNUoS Tariffs Report |

The slim GeoJSON files in `data/` have been geometrically simplified for browser delivery. The full-resolution files used by the Python script are kept out of the repo (see `.gitignore`).

## Methodology summary

- **DUoS region**: lat/lon transformed to EPSG:27700 (British National Grid), then point-in-polygon against the DNO licence area layer.
- **TNUoS demand zone**: GSP group lookup against the NESO GSP group polygons, then mapped to the numbered demand zone (1 to 14) via the published GSP group → demand zone table.
- **TNUoS generation zone**: point-in-polygon against the 27 generation zone polygons (GZ1 to GZ27).
- **Bands**: Site Specific bands and Residual bands are derived from the published capacity / annual energy thresholds for the relevant DUoS basis (LV no MIC, LV MIC, HV, EHV) and TNUoS T-Demand banding.

## Disclaimer

See [DISCLAIMER.md](DISCLAIMER.md). Short version: this is an exploratory aid, not a regulated tool. **Do not rely on its output for billing, contract, or settlement decisions.**

## Licence

MIT. See [LICENSE](LICENSE).
