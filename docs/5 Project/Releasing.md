---
tags: [project]
---

# Releasing

## NuGet (core)

1. Bump `<Version>` in `src/NetSync/NetSync.csproj` and date the release in `CHANGELOG.md`. Protocol v4 is frozen: new wire behaviour goes behind a feature bit ([Handshake v4](../3%20Internals/Handshake%20v4.md)), never a new `PeerProtocol.ProtocolVersion`.
2. `dotnet pack src/NetSync -c Release` → `.nupkg` + `.snupkg` (README embedded). Package validation fails the pack when the public API breaks compatibility with the baseline release (`PackageValidationBaselineVersion`, 2.0.0-rc.5).
3. Tag `v<version>` and push — `.github/workflows/release.yml` tests, packs and pushes through trusted publishing: no stored key (nuget.org → Trusted Publishing policy for this repository and `release.yml`; the profile name is `NUGET_USER` in the workflow). `ci.yml` builds both targets with warnings as errors, runs both suites on .NET 8 and .NET 10 on Windows, Linux and macOS, checks the docs' links, packs.
4. NativeAOT smoke test: `dotnet publish samples/Echo -c Release -r win-x64 -p:AotSample=true`.

## Unity package (core)

The same tag publishes `com.netsync.core` to npmjs.com, where Unity's Package Manager reads it as a scoped registry. `tools/pack-unity.py` wraps the netstandard2.1 DLL in a Unity package with generated `.meta` files (stable GUIDs, nothing Unity-specific in the repository), and `npm publish` sends it through trusted publishing: no token. The `.tgz` is attached to the GitHub Release as well.

One-time setup: publish the first version by hand (`npm publish com.netsync.core-<version>.tgz --access public`), then npmjs.com → `com.netsync.core` → Settings → Trusted Publisher → GitHub Actions, `VladislavKresov/NetSync`, `release.yml`.

## Unity adapter (sibling repo)

The adapter depends on `com.netsync.core` instead of carrying a copy of the DLL.

1. Raise `"com.netsync.core"` in its `package.json` when it needs a newer core (Unity takes exact versions, not ranges; a project can always ask for a newer core itself).
2. Bump `version`, commit, tag.
3. Test in real Unity (Mono and IL2CPP), with the core resolved from the registry.
