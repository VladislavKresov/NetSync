"""Builds the Unity package com.netsync.core from an already built netstandard2.1 NetSync.dll.

    python tools/pack-unity.py --dll src/NetSync/bin/Release/netstandard2.1 --version 2.0.0 --out <folder>

The folder is a ready Unity Package Manager package: `npm pack <folder>` or `npm publish <folder>`.
Unity needs a .meta file next to every file of a package it installs from a registry; they are
generated here, with GUIDs derived from the file path so they stay the same from version to version.
Nothing Unity-specific is kept in the repository itself.
"""
import argparse
import hashlib
import json
import os
import re
import shutil

PACKAGE = "com.netsync.core"
REPOSITORY = "https://github.com/VladislavKresov/NetSync"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def guid(path):
    return hashlib.md5(f"{PACKAGE}/{path}".encode("utf-8")).hexdigest()


def write(path, text):
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(text)


def meta(out, path, importer):
    write(os.path.join(out, path + ".meta"), f"fileFormatVersion: 2\nguid: {guid(path)}\n{importer}")


FOLDER = "folderAsset: yes\nDefaultImporter:\n  externalObjects: {}\n  userData: \n  assetBundleName: \n  assetBundleVariant: \n"
TEXT = "TextScriptImporter:\n  externalObjects: {}\n  userData: \n  assetBundleName: \n  assetBundleVariant: \n"
MANIFEST = "PackageManifestImporter:\n  externalObjects: {}\n  userData: \n  assetBundleName: \n  assetBundleVariant: \n"
PLUGIN = """PluginImporter:
  externalObjects: {}
  serializedVersion: 2
  iconMap: {}
  executionOrder: {}
  defineConstraints: []
  isPreloaded: 0
  isOverridable: 0
  isExplicitlyReferenced: 0
  validateReferences: 1
  platformData:
  - first:
      Any:
    second:
      enabled: 1
      settings: {}
  userData:
  assetBundleName:
  assetBundleVariant:
"""

README = f"""# NetSync Core

The [NetSync]({REPOSITORY}) networking core as a Unity package: one zero-dependency .NET Standard 2.1 DLL.

Most projects install [NetSync for Unity](https://github.com/VladislavKresov/NetSync.Unity), which depends on this package
and adds the Unity integration. Install this one alone only to use the core API directly.
"""


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dll", required=True, help="folder with the netstandard2.1 NetSync.dll and NetSync.xml")
    parser.add_argument("--version", required=True, help="package version, the same as the core's <Version>")
    parser.add_argument("--out", required=True, help="folder to create (must not exist)")
    args = parser.parse_args()

    if not re.fullmatch(r"\d+\.\d+\.\d+(-[0-9A-Za-z.-]+)?", args.version):
        parser.error(f"not a SemVer version: {args.version}")
    if os.path.exists(args.out):
        parser.error(f"{args.out} already exists")

    out = args.out
    os.makedirs(os.path.join(out, "Runtime"))
    for name in ("NetSync.dll", "NetSync.xml"):
        shutil.copyfile(os.path.join(args.dll, name), os.path.join(out, "Runtime", name))
    shutil.copyfile(os.path.join(ROOT, "LICENSE"), os.path.join(out, "LICENSE.md"))
    write(os.path.join(out, "README.md"), README)
    manifest = {
        "name": PACKAGE,
        "version": args.version,
        "displayName": "NetSync Core",
        "description": "Zero-dependency networking core: TCP and UDP channels over one connection, reliable UDP, "
                       "authentication and encryption, relay with hole punching, time sync, LAN discovery.",
        "unity": "2021.2",
        "license": "MIT",
        "author": {"name": "Vladislav Kresov"},
        "homepage": REPOSITORY,
        "repository": {"type": "git", "url": f"git+{REPOSITORY}.git"},
        "keywords": ["networking", "multiplayer", "tcp", "udp", "relay", "netsync"],
    }
    write(os.path.join(out, "package.json"), json.dumps(manifest, indent=2) + "\n")

    meta(out, "package.json", MANIFEST)
    meta(out, "README.md", TEXT)
    meta(out, "LICENSE.md", TEXT)
    meta(out, "Runtime", FOLDER)
    meta(out, "Runtime/NetSync.dll", PLUGIN)
    meta(out, "Runtime/NetSync.xml", TEXT)
    print(f"{PACKAGE} {args.version} -> {out}")


if __name__ == "__main__":
    main()
