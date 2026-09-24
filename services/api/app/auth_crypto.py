from __future__ import annotations

import hashlib
import hmac
import secrets


_SCRYPT_N = 16384
_SCRYPT_R = 8
_SCRYPT_P = 1
_SALT_BYTES = 16
_HASH_BYTES = 32


def hash_password(password: str) -> str:
    salt = secrets.token_bytes(_SALT_BYTES)
    derived = hashlib.scrypt(
        password.encode("utf-8"), salt=salt,
        n=_SCRYPT_N, r=_SCRYPT_R, p=_SCRYPT_P, dklen=_HASH_BYTES,
    )
    return f"scrypt${_SCRYPT_N}${_SCRYPT_R}${_SCRYPT_P}${salt.hex()}${derived.hex()}"


def verify_password(password: str, encoded: str) -> bool:
    if not isinstance(encoded, str):
        return False
    fields = encoded.split("$")
    if len(fields) != 6 or fields[:4] != ["scrypt", "16384", "8", "1"]:
        return False
    try:
        salt = bytes.fromhex(fields[4])
        expected = bytes.fromhex(fields[5])
    except ValueError:
        return False
    if (
        len(salt) != _SALT_BYTES or salt.hex() != fields[4]
        or len(expected) != _HASH_BYTES or expected.hex() != fields[5]
    ):
        return False
    actual = hashlib.scrypt(
        password.encode("utf-8"), salt=salt,
        n=_SCRYPT_N, r=_SCRYPT_R, p=_SCRYPT_P, dklen=_HASH_BYTES,
    )
    return hmac.compare_digest(actual, expected)
