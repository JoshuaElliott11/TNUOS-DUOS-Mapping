# Disclaimer

This is a personal, exploratory project. It is not a production tool, it is not a certified network-charge calculator, and it has no affiliation with NESO, Ofgem, Elexon, any DNO, or any other GB energy industry body.

## What this tool is

A teaching and scoping aid. You give it a latitude, longitude, and a brief description of the connection, and it returns the DUoS and TNUoS line items that probably apply, with the bands they probably fall into.

## What it isn't

**Authoritative on region.** What the bill actually follows is the DUoS region your MPAN is registered to, not the polygon a coordinate happens to fall inside. Sites near a boundary, and sites connected via a non-local network, will often disagree with the spatial lookup.

**Authoritative on tariff selection.** The recommended charge set is a generalised mapping from site archetype to line items. The lines that turn up on a real connection agreement, bill, or NESO settlement run depend on the specific connection, the charging methodology in force, any derogations, and DNO-specific overrides.

**Authoritative on bands.** Band thresholds for Site Specific, Residual, and T-Demand charges are taken from the public charging methodology at the time of writing. They move at each charging review and at each tariff year. Verify against the current CDCM, CCCM, and the latest NESO TNUoS tariff publication before using the bands in a model.

**A calculator.** The tool identifies which line items apply. It does not multiply tariffs by volumes.

## Data freshness

The GeoJSON layers in `data/` are snapshots from public NESO sources, dated in the filenames. They are not refreshed automatically. The bundled `Public - 2026-27 Final TNUoS Tariffs Report` and supporting documents are point-in-time references.

Use the tool to scope and orient a piece of work. Validate against the live source documents before committing to anything.

## No reliance

Output from this tool must not be used as the basis for billing, settlement, contract negotiation, regulatory filings, or any other document where accuracy matters. The author accepts no liability for decisions, omissions, or losses arising from its use.

If you need authoritative network-charge advice for a specific site, talk to the DNO connection team, your supplier, or a qualified network-charges advisor.
