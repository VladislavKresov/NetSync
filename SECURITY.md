# Security policy

## Supported versions

Security fixes go into the latest release. Until 2.0.0 is final that is the latest release candidate.

## Reporting a vulnerability

Please do not open a public issue for a security problem.

Report it privately through GitHub: **Security → Report a vulnerability** on this repository
(<https://github.com/VladislavKresov/NetSync/security/advisories/new>). Include what you observed, the NetSync version and
runtime, and the smallest way to reproduce it that you have.

You can expect an answer within a few days. Once a fix is released the report is published as a security advisory, with
credit to the reporter unless you prefer otherwise.

## What NetSync protects, and what it does not

The [security guide](docs/2%20Guides/Security.md) describes the protection each configuration gives. In short: the default
configuration authenticates and encrypts nothing and is meant for trusted networks; a pre-shared key that ships inside a
client is known to everyone who has the client; for anything else use `Ecdh` with a pinned server identity.
