# Code and data provenance

This is a new Git repository with no Olympus history or application dependencies. The validated shadow service, bounded archive acquisition, source quality validator, and public feed measurement tools were adapted from work prepared in `rstl2/olympus` PR 1462, source commit `41fcec1f3a2294cea74f1bc8ece438889c5537c0`. The deterministic order core, emulator, standalone operator console and dashboard are new components.

The TypeScript execution file under `tools/reference` is a development oracle, not the Rust production core. The archive quality validator is adapted for Node's built-in TypeScript transform, with filesystem/utility calls replaced and compact 5m/15m market identity explicitly scoped. Research tools run separately and are absent from the engine image.

Pendulumflow V3 data is CC BY 4.0. Attribute **pendulumflow**, link https://archive.pendulumflow.com/ and retain native provenance and hashes. See https://archive.pendulumflow.com/credits. No third-party archive corpus or credentials are committed. Source availability is separate from lossless capture, point-in-time availability and trading eligibility.

Polymarket protocols must be pinned together (endpoint, asset identifier, collateral, exchange domain). Official SDK: https://github.com/Polymarket/rs-clob-client-v2. No implemented gateway is authorized to submit real orders in this repository's default modes.
