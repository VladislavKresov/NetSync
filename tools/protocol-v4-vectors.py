"""Recomputes the protocol v4 reference vectors independently of the library (big-integer X25519, hashlib HKDF).

The values printed here are pinned in tests/NetSync.Tests/ProtocolV4VectorsTests.cs.
"""
import hashlib
import hmac

P = 2 ** 255 - 19


def x25519(k: bytes, u: bytes) -> bytes:
    k = bytearray(k)
    k[0] &= 248
    k[31] &= 127
    k[31] |= 64
    scalar = int.from_bytes(k, "little")
    x1 = int.from_bytes(u, "little") & ((1 << 255) - 1)
    x2, z2, x3, z3, swap = 1, 0, x1, 1, 0
    for t in reversed(range(255)):
        bit = (scalar >> t) & 1
        swap ^= bit
        if swap:
            x2, x3, z2, z3 = x3, x2, z3, z2
        swap = bit
        a, b = (x2 + z2) % P, (x2 - z2) % P
        aa, bb = a * a % P, b * b % P
        e = (aa - bb) % P
        c, d = (x3 + z3) % P, (x3 - z3) % P
        da, cb = d * a % P, c * b % P
        x3 = (da + cb) ** 2 % P
        z3 = x1 * (da - cb) ** 2 % P
        x2 = aa * bb % P
        z2 = e * (aa + 121665 * e) % P
    if swap:
        x2, x3, z2, z3 = x3, x2, z3, z2
    return (x2 * pow(z2, P - 2, P) % P).to_bytes(32, "little")


def hkdf(ikm: bytes, salt: bytes, info: bytes, length: int) -> bytes:
    prk = hmac.new(salt, ikm, hashlib.sha256).digest()
    out, block, counter = b"", b"", 1
    while len(out) < length:
        block = hmac.new(prk, block + info + bytes([counter]), hashlib.sha256).digest()
        out += block
        counter += 1
    return out[:length]


base = (9).to_bytes(32, "little")
c_priv = bytes.fromhex("77076d0a7318a57d3c16c17251b26645df4c2f87ebc0992ab177fba51db92c2a")
s_priv = bytes.fromhex("5dab087e624a8a4b79e17f8b83800ee66f3bb1292618b6fd1c2f8b27ff88e0eb")
id_priv = bytes([0x55]) * 32
c_pub, s_pub, id_pub = x25519(c_priv, base), x25519(s_priv, base), x25519(id_priv, base)
ee, es = x25519(c_priv, s_pub), x25519(c_priv, id_pub)


def block(b):
    return bytes([len(b)]) + b


def transcript(key_exchange, identity):
    return (b"NetSync/4 handshake" + bytes.fromhex("0102030405060708") + bytes.fromhex("a1b2c3d4") + bytes([key_exchange, 3]) +
            bytes.fromhex("1122334455667788") + bytes.fromhex("0007") + bytes(4) + block(c_pub) + bytes([1]) + bytes(4) +
            block(s_pub) + block(identity))


def secret(tr, material):
    return hkdf(material, hashlib.sha256(tr).digest(), b"NetSync/4 session", 32)


print("idpub", id_pub.hex())
print("transcript", transcript(2, id_pub).hex())
print("secret-identity", secret(transcript(2, id_pub), ee + es).hex())
print("secret-psk", secret(transcript(1, b""), ee + b"0123456789abcdef0123456789abcdef").hex())

session = bytes.fromhex(secret(transcript(2, id_pub), ee + es).hex())
auth_c2s = hmac.new(session, b"NetSync/4 auth c2s 1", hashlib.sha256).digest()
inner = bytes([0x10, 0x02]) + b"hello"
counter = (1).to_bytes(8, "big")
tag = hmac.new(auth_c2s, bytes([1]) + counter + inner, hashlib.sha256).digest()[:16]
print("envelope", (bytes([0x40]) + counter + tag + inner).hex())
