---
tags: [project]
---

# Releasing

## NuGet (core)

1. Bump `<Version>` in `src/NetSync/NetSync.csproj`; wire-breaking changes also bump `PeerProtocol.ProtocolVersion` ([[PeerProtocol]]).
2. `dotnet pack src/NetSync -c Release` → `.nupkg` + `.snupkg` (README embedded).
3. Tag `v<version>` and push — `.github/workflows/release.yml` tests, packs and pushes through trusted publishing: no stored key (nuget.org → Trusted Publishing policy for this repository and `release.yml`; the profile name is `NUGET_USER` in the workflow). `ci.yml` builds both targets with warnings as errors, runs both suites on Windows and Linux, uploads the package.
4. NativeAOT smoke test: `dotnet publish samples/Echo -c Release -r win-x64 -p:AotSample=true`.

## Unity package (core)

The same tag publishes `com.netsync.core` to npmjs.com, where Unity's Package Manager reads it as a scoped registry. `tools/pack-unity.py` wraps the netstandard2.1 DLL in a Unity package with generated `.meta` files (stable GUIDs, nothing Unity-specific in the repository), and `npm publish` sends it through trusted publishing: no token. The `.tgz` is attached to the GitHub Release as well.

One-time setup: publish the first version by hand (`npm publish com.netsync.core-<version>.tgz --access public`), then npmjs.com → `com.netsync.core` → Settings → Trusted Publisher → GitHub Actions, `VladislavKresov/NetSync`, `release.yml`.

## Unity adapter (sibling repo)

The adapter depends on `com.netsync.core` instead of carrying a copy of the DLL.

1. Raise `"com.netsync.core"` in its `package.json` when it needs a newer core (Unity takes exact versions, not ranges; a project can always ask for a newer core itself).
2. Bump `version`, commit, tag.
3. Test in real Unity (Mono and IL2CPP).

Pending adapter work for 2.2: [[Roadmap#Unity adapter]].
