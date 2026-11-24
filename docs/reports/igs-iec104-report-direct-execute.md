# Draft report for the owner of `igs-iec104`: direct execute refused before the handler

Status: **draft, not sent.** Nothing has been posted to the library's repository. Review it, then decide whether it goes out.

## Summary

A single command or double command with S/E = 0 (direct execute, no prior select) is answered with a negative activation confirmation by the station, before the server handler is called. The handler accepts every command in the reproduction, so the refusal comes from the library, not from the application.

## Environment

- `igs-iec104` 0.1.0, used with `default-features = false`.
- Loopback station (`Server::bind` on `127.0.0.1:0`, common address 1), one point at IOA 5, a controlling `Client`.

## Reproduction

The reproduction is a test of the Powermug repository, at commit `65f0f0a` or later:

`crates/powermug-adapter-iec104/tests/command.rs`, test `a_direct_execute_is_refused_by_the_station_before_its_handler_without_a_selection`.

Steps of the test:

1. Start a station whose handler accepts `select` and `execute`.
2. Connect a controlling client and wait for the data transfer to start.
3. Send a single command, address 5, value ON, S/E = 0 (no select first).
4. Read the answer.

## Expected

The handler is called and the station answers with an activation confirmation, then an activation termination (the handler accepted the execution).

## Observed

- The station answers with a negative confirmation (cause of transmission "activation", negative flag set) for IOA 5.
- The handler is never called: the recorded executions are empty.

The same station, with a select (S/E = 1) followed by the execution (S/E = 0), works: confirmation, then termination.

## Impact

Powermug's core can send direct commands. Through `igs-iec104`, every direct command is answered with a refusal and reported as equipment rejected. Only the select-before-operate sequence reaches the field. This is recorded as gap G-6 in the Powermug repository (`docs/reports/igs-iec104-gaps.md`).

## Question for the owner

1. Is refusing a direct execution without a prior select the intended behaviour of the station? IEC 60870-5-104 allows direct execution; select-before-operate is an application choice.
2. If the behaviour is not intended, can the station hand a direct execution to its handler, and is a fix planned?
3. If it is intended, please document it, so that Powermug can select first on its side.

## Not covered

- Double commands with S/E = 0 are not reproduced here: the reproduction covers single commands only. Set points were not checked for this behaviour.
- The client side is not affected: the reproduction sends from the controlling client.
