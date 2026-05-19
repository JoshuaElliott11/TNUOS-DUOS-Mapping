# TNUoS + DUoS Mapping

A single-page web app, plus a Python companion script, that takes a latitude and longitude in Great Britain and tells you which DUoS region, TNUoS demand zone, and TNUoS generation zone the site sits in. It then lists the DUoS and TNUoS line items you should expect on the bill.

Live site: https://joshuaelliott11.github.io/TNUOS-DUOS-Mapping/

## What it does

Network charges on a GB site come from two regimes.

DUoS (Distribution Use of System) is set by the 14 DNO licence areas and charged on distribution-connected sites. TNUoS (Transmission Network Use of System) is set by NESO across demand zones (for import) and generation zones (for export), and applies to both transmission-connected sites and embedded ones.

Which lines actually apply on a given site depends on two things: where the site is on the map, and what kind of site it is (pure demand, pure generation, storage, co-located, distribution or transmission-connected). The tool answers both.

There are two modes.

The first is a region lookup. The supplied lat/lon is ray-cast against three GeoJSON layers: DNO licence areas (in EPSG:27700), TNUoS demand GSP groups, and TNUoS generation zones. You get back the DUoS operator, demand zone, GSP code, and generation zone.

The second is a tariff recommender. Pick one of ten site archetypes and the tool produces the matching set of DUoS and TNUoS line items, with Site Specific, Residual, Triad, Embedded Export, and Wider Generation labels filled in for you. Band suggestions follow from the capacity or annual energy you supply.

## How to use it

1. Open the live site, or `index.html` locally.
2. Enter latitude and longitude in decimal degrees (WGS84).
3. Pick a site archetype.
4. Enter capacity (kVA or kW) or annual energy (kWh or MWh).
5. Click Lookup Regions for the region summary, or Recommend Tariffs for the full charge set.

The Python script `tnuos_duos_lookup.py` does the same region lookup against the full-resolution NESO GeoJSON files (rather than the slim versions used by the web app) and handles single-point and batch CSV or XLSX lookups:

```
python tnuos_duos_lookup.py --lat 52.2053 --lon 0.1218
python tnuos_duos_lookup.py --input sites.csv --output sites_with_regions.csv
```

It needs `pyproj`. For Excel mode it also needs `openpyxl`.

## Data sources

| Layer | Source |
| --- | --- |
| DNO licence areas | NESO open data, DNO licence area boundaries (EPSG:27700) |
| GSP groups | NESO open data, GSP group polygons |
| TNUoS generation zones | NESO TNUoS generation zone polygons |
| 2026/27 TNUoS tariffs | NESO 2026/27 Final TNUoS Tariffs Report |

The slim GeoJSON files in `data/` have been simplified for browser delivery. The full-resolution versions used by the Python script are kept out of the repo (see `.gitignore`).

## Methodology summary

DUoS region. Lat/lon is reprojected into EPSG:27700 (British National Grid) and then point-in-polygon against the DNO licence area layer.

TNUoS demand zone. GSP group lookup against the NESO GSP group polygons, mapped to the numbered demand zone (1 to 14) via the published GSP group / demand zone table.

TNUoS generation zone. Point-in-polygon against the 27 generation zone polygons (GZ1 to GZ27).

Bands. Site Specific and Residual bands are read off the published capacity / annual energy thresholds for the relevant DUoS basis (LV no MIC, LV MIC, HV, EHV) and the TNUoS T-Demand bands.

## Disclaimer

See [DISCLAIMER.md](DISCLAIMER.md). The tool is exploratory. Output should not be used to set bills, sign contracts, or settle volumes.

## Licence

MIT. See [LICENSE](LICENSE).
