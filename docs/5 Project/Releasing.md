---
tags: [project]
---

# Releasing

## NuGet (core)

1. Bump `<Version>` in `src/NetSync/NetSync.csproj`; wire-breaking changes also bump `PeerProtocol.ProtocolVersion` ([[PeerProtocol]]).
2. `dotnet pack src/NetSync -c Release` → `.nupkg` + `.snupkg` (README embedded).
3. Tag `v<version>` and push — `.github/workflows/release.yml` tests, packs and pushes with the `NUGET_API_KEY` secret. `ci.yml` builds both targets with warnings as errors, runs both suites on Windows and Linux, uploads the package.
4. NativeAOT smoke test: `dotnet publish samples/Echo -c Release -r win-x64 -p:AotSample=true`.

## Before the first publish (open)

- [ ] Real `RepositoryUrl` / `PackageProjectUrl` in `NetSync.csproj` (placeholder `your-name`) and the clone URL in README.
- [ ] Check that the NuGet id `NetSync` is free.
- [ ] Add the `NUGET_API_KEY` secret; create the GitHub remotes (none yet).
- [ ] Tag `v2.0.0-rc.3`.
- [ ] Open the Unity package in a real Unity 2021.2+ once.

## Unity adapter (sibling repo)

1. `powershell -File tools~/update-core.ps1` — rebuilds the core, refreshes the DLL.
2. `dotnet build tools~/CompileCheck/CompileCheck.csproj` — compiles adapter + samples against UnityEngine stubs at C# 9.
3. Bump `version` in `package.json` (same as NuGet), commit, tag.
4. Test in real Unity (Mono and IL2CPP) — the stub check can't catch runtime behaviour.

Pending adapter work for 2.2: [[Roadmap#Unity adapter]].
