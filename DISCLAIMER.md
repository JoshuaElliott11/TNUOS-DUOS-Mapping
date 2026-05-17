# Disclaimer

This is a personal, exploratory project. **It is not a production tool, not a certified network-charge calculator, and not affiliated with NESO, Ofgem, Elexon, any DNO, or any other GB energy industry body.**

## What this tool is

A teaching and scoping aid for working out, from a latitude/longitude and a site description, which DUoS and TNUoS line items are likely to apply and which bands they are likely to fall into.

## What this tool is not

- **Not authoritative on region**. The DUoS region your MPAN is assigned to is the source of truth for billing — not the polygon a coordinate happens to fall inside. Sites near a boundary, or sites connected via a non-local network, will commonly differ from the spatial lookup.
- **Not authoritative on tariff selection**. The recommended tariff set is a generalised mapping from site archetype to charge lines. The actual lines on a real connection agreement, bill, or NESO settlement run will depend on the specific connection, network charging code provisions, derogations, and any DNO-specific overrides in force at the time.
- **Not authoritative on bands**. Band thresholds for Site Specific, Residual, and T-Demand charges are codified from the public charging methodology at the time of writing. They are subject to change at each charging review and at each tariff year. The bands returned here are illustrative — verify against the current Common Distribution Charging Methodology (CDCM), Common Connection Charging Methodology (CCCM), and the latest NESO TNUoS tariff publication before using in a commercial model.
- **Not a calculator**. This tool does not multiply tariffs by volumes. It identifies which line items apply.

## Data freshness

- The GeoJSON layers in `data/` are snapshots from public NESO sources at the dates indicated in the filenames. They are not refreshed automatically.
- The bundled `Public - 2026-27 Final TNUoS Tariffs Report` and supporting documents are public, point-in-time references.
- Use this tool to scope and orient a piece of work, then validate against the current source documents.

## No reliance

Output from this tool **must not** be used as the basis for billing, settlement, contract negotiation, regulatory filings, or any other document where accuracy matters. The author accepts no liability for any decisions, omissions, or losses arising from its use.

If you need authoritative network-charge advice for a specific site, consult the relevant DNO connection team, your supplier, or a qualified network-charges advisor.
