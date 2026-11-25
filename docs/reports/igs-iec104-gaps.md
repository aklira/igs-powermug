# igs-iec104 — inventory against Powermug needs

- **Date:** 2026-11-19
- **Phase:** P0, gap inventory ([IMPLEMENTATION.md](../../specifications/IMPLEMENTATION.md) §4, P0 and §6.1)
- **Library:** `igs-iec104` 0.1.0 (`igs-iec104`, `igs-iec104-codec`, `igs-iec104-link`), Apache-2.0
- **Sources read:** README, `docs/capabilities.md`, `CHANGELOG.md` (Known gaps), `docs/ai-log/S3.md`, and the server and configuration code.

This inventory does not modify the `igs-iec104` repository. The gaps below are to be opened in its backlog once the owner agrees.

## Features needed and their status

| Need | Source | Status in igs-iec104 | Phase |
|---|---|---|---|
| Controlling station (client), general interrogation | FUN-ACQ-010, UC-10 | Yes, tested | P2 |
| Spontaneous and time-tagged data reception | FUN-ACQ-004 | Yes, every profile type decoded | P2 |
| Controlled station (server), GI answer from the image | FUN-IMG-003 | Yes, plain and time-tagged point types | P2 |
| Single command, double command, set-point (normalized, scaled, float) | MVP TC (`C_DC_NA_1`) and TVC (`C_SE_NC_1`) | Yes, with and without time tag | P3 |
| Select-before-operate | UC-05 | Yes: selection kept up to its time-out | P3 |
| Direct execution without selection | MVP S17 | Client sends it; server refuses it when it has no selection (correct for SBO-only points) | P3 |
| Clock synchronization | Time service | Yes, both sides | P2 |
| Redundancy group, client side | MVP decision (one group per SCADA) | Yes (`RedundantClient`) | P2 |
| Redundancy group, server side, one active connection | MVP decision, S16 | Yes, with switchover and retransmission of unacknowledged events | P2 |
| Reconnection with back-off | FUN-ACQ-006 | Yes (client) | P2 |
| TLS with client certificate (IEC 62351-3) | NFR-SEC-001, §11.6 | Yes, OpenSSL backend (default); rustls backend lacks parts | P5–P7 |
| Stop with grace period | Shutdown of a cell | Yes (`Server::run_until`) | P2 |
| Connection events for diagnostics | FUN-OPS-001 | Yes, with bounded stream | P2 |
| Bitstring command | Not in MVP | Not offered | — |
| Reset process | Not in MVP | Default refuses it; handler can accept | — |
| Parameters (`P_ME_*`) | Not in MVP | Not offered | — |
| File transfer (`F_*`) | Not in scope | Not implemented | — |
| Protection events (`M_EP_*`) | Out of scope (§3.3) | Not published | — |

## Gaps

| # | Gap | Impact | Blocks | Decision needed |
|---|---|---|---|---|
| **G-1** | **One station per listener.** A server has one common address (`ServerConfig::common_address`). Requests to another address are mirrored with cause 46. The process image is keyed by common address internally, but no API serves several stations on one listener. `S3.md` records the choice: several stations are several servers on several ports. | Blocks the `CELL_DC1` model: eleven stations behind one SCADA gateway, with one redundancy group ([AST] §12.6; PM-ACC-013). With one server per station, the SCADA needs eleven connections per redundancy side. | P5 (cell scope), PM-ACC-013 | **Yes.** Either (a) add a multi-station listener to `igs-iec104` with routing by common address on one connection, or (b) accept eleven ports per cell and change the MVP model. Recommendation: (a), because it keeps the single redundancy group of the MVP. |
| **G-2** | **Interop, load and switchover-under-load tests not done.** `CHANGELOG.md` lists them as known gaps. | Required before claiming ACC-003, ACC-005 (on the IEC 104 side) and ACC-016 with confidence. | P2 exit (interop), P8 (switchover under load) | Owner to plan the interop campaign with the bench peers (lib60870 used only as an independent peer, ADR-02). |
| **G-3** | **TLS on the rustls backend.** Missing mandatory CBC and DHE suites of TLS 1.2, CCM, ffdhe2048, `rsa_pss_pss_sha256`, renegotiation, and TLS 1.3 key update. | Powermug should use the OpenSSL backend. This adds a native dependency (`libssl`), to be packaged in the container. | P5–P7 (security matrix, NFR-SEC-010) | No, if OpenSSL is accepted. Confirm the OpenSSL 3 licence (Apache-2.0) in `cargo-deny` and the SBOM. |
| **G-4** | **11 of 36 security events of Annex A not raised** by the OpenSSL backend (feature not implemented or not offered). | Affects the IEC 62351-3 evidence in the compliance matrix. | P7, P9 | Owner to decide which events are required for the production claim. |
| **G-5** | **Bench scenarios for TLS not written.** The reference plugin's TLS configuration is not in the repository. | Cannot run the secured MVP scenarios. | P7 | Obtain the reference TLS configuration. |
| **G-6** | **Direct execute refused without a prior selection.** A single or double command with S/E = 0 is answered with a negative confirmation by the station, before its handler is called, even when the handler accepts every command. Reproduced on loopback: `crates/powermug-adapter-iec104/tests/command.rs`, `a_direct_execute_is_refused_by_the_station_before_its_handler_without_a_selection` (pins the behaviour). Select-before-operate works (tests of the same file). | The core's `direct_execute` operation (SPECIFICATIONS 9.3, FUN-CMD) cannot reach the field through `igs-iec104`: every direct command is answered `EQUIPMENT_REJECTED`. Select-before-operate is the only path that works today. | P3 (direct commands), P7 | Owner to confirm whether the station must accept a direct execution (IEC 60870-5-104 allows it) and fix it in the library, or whether Powermug must always select first. Not raised on the library's tracker yet: the report needs the owner's approval. |
| **G-7** | **The station's handler does not know which connection an operation comes from.** `Handler::select` and `Handler::execute` receive the `Operation` only (address, value, time). The peer of the connection (its address, and the common name of its TLS client certificate) is reported as a server event, but not to the handler, so a command cannot be authorized by the identity of its master. | MVP S18 (a second IEC 104 master, absent from the model, is refused) cannot be done faithfully: a single issuer would hide the second master. | P3 (S18), P4 (North IEC 104 entry) | Owner to confirm the change: pass the connection's peer identity with the operation (or a per-connection handler). Not raised on the library's tracker yet: the report needs the owner's approval. Also: the TLS backend is needed for an identity from the certificate; the decision is `openssl` (feature `openssl`, native libssl, not yet enabled since nothing uses it). |



## What is not a gap

- **The client keeps no values.** Powermug builds its own process image from `Delivery::Asdu`. This is by design (FUN-IMG-001).
- **The server does not send the answers of a closed connection on a new one** (fixed in CHANGELOG). Covered by the switchover tests.
- **One server is one redundancy group.** This is the chosen model, not a defect, but it is the root of G-1.

## Follow-ups on Powermug documents

- SPECIFICATIONS.md §6.7 lists `iec104-server` as one adapter instance. With G-1 open, a server instance may need to carry several stations. The specification must say which option is retained once the owner decides.
