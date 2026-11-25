# Draft report for the owner of `igs-iec104`: the handler does not know the connection of an operation

Status: **draft, not sent.** Nothing has been posted to the library's repository. Review it, then decide whether it goes out.

## Summary

`Handler::select` and `Handler::execute` receive the `Operation` only: its address, its value and its time. The peer of the connection the operation comes from is reported as a server event, but never to the handler. An application that authorizes commands by the identity of their issuer (for example the common name of the TLS client certificate of a controlling station) cannot tell which controlling station sent an operation.

## Environment

- `igs-iec104` 0.1.0, with the `openssl` feature (the default), as the owner's decision requires for IEC 62351-3 identity.
- Server side: `Server::bind` with a `Handler`; controlling stations connect over TLS.

## Why it matters

The MVP scenario S18 requires that a second IEC 104 master, absent from the model, is refused by the cell and traced in its journal. Without the connection of an operation, the application has only two choices: accept every master under one issuer (the second master is not told apart from the first), or refuse masters at the transport level (no trace of the command in the application journal).

## Expected

The operation handed to the handler carries the identity of its connection: the peer address and, when TLS is used, the common name of the client certificate. Either the `Operation` gets that field, or the handler is created per connection.

## Observed

- `src/server/respond.rs` (`igs-iec104` 0.1.0), trait `Handler`: `select(&self, operation: &Operation)` and `execute(&self, operation: &Operation)` receive no connection context. `Operation` holds `address`, `value` and `time` only.
- `ServerEvent::Connected { id, peer }` is reported to the event stream, with no link to the later operations.

## Question for the owner

1. Is it intended that a handler cannot know the connection of an operation?
2. If not, can the operation carry the peer identity, or can the handler be created per connection, with the TLS identity of the peer?

## Not covered

- The TLS side is not tested here: the feature is not enabled in Powermug yet.
- The reproduction is by reading the API, not by a test. A test would need a second controlling station with its own certificate.
