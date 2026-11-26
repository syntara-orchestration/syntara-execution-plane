"""Ephemeral mutual TLS credentials for one SDK workload attempt."""

from __future__ import annotations

import base64
import ipaddress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID


@dataclass(frozen=True)
class RuntimeTlsMaterial:
    """Certificate material kept in EP memory and mounted only into its workload."""

    ca_certificate: bytes
    server_certificate: bytes
    server_key: bytes
    client_certificate: bytes
    client_key: bytes

    @classmethod
    def create(cls) -> RuntimeTlsMaterial:
        """Issue a short lived CA, server certificate, and EP client certificate."""
        # CertificateBuilder's portable API accepts naive UTC values.
        now = datetime.now(UTC).replace(tzinfo=None)
        not_before = now - timedelta(minutes=5)
        not_after = now + timedelta(days=2)
        ca_key = ec.generate_private_key(ec.SECP256R1())
        ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "syntara-ep-runtime-ca")])
        ca_certificate = (
            x509.CertificateBuilder()
            .subject_name(ca_name)
            .issuer_name(ca_name)
            .public_key(ca_key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(not_before)
            .not_valid_after(not_after)
            .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
            .add_extension(
                x509.KeyUsage(
                    digital_signature=True,
                    content_commitment=False,
                    key_encipherment=False,
                    data_encipherment=False,
                    key_agreement=False,
                    key_cert_sign=True,
                    crl_sign=True,
                    encipher_only=False,
                    decipher_only=False,
                ),
                critical=True,
            )
            .sign(ca_key, hashes.SHA256())
        )

        def issue_leaf(common_name: str, usage: x509.ObjectIdentifier) -> tuple[x509.Certificate, bytes]:
            key = ec.generate_private_key(ec.SECP256R1())
            subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
            builder = (
                x509.CertificateBuilder()
                .subject_name(subject)
                .issuer_name(ca_name)
                .public_key(key.public_key())
                .serial_number(x509.random_serial_number())
                .not_valid_before(not_before)
                .not_valid_after(not_after)
                .add_extension(
                    x509.KeyUsage(
                        digital_signature=True,
                        content_commitment=False,
                        key_encipherment=False,
                        data_encipherment=False,
                        key_agreement=False,
                        key_cert_sign=False,
                        crl_sign=False,
                        encipher_only=False,
                        decipher_only=False,
                    ),
                    critical=True,
                )
                .add_extension(x509.ExtendedKeyUsage([usage]), critical=False)
            )
            if usage == ExtendedKeyUsageOID.SERVER_AUTH:
                builder = builder.add_extension(
                    x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]),
                    critical=False,
                )
            certificate = builder.sign(ca_key, hashes.SHA256())
            private_key = key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
            return certificate, private_key

        server_certificate, server_key = issue_leaf("syntara-ep-runtime-server", ExtendedKeyUsageOID.SERVER_AUTH)
        client_certificate, client_key = issue_leaf("syntara-ep-runtime-client", ExtendedKeyUsageOID.CLIENT_AUTH)
        return cls(
            ca_certificate=ca_certificate.public_bytes(serialization.Encoding.PEM),
            server_certificate=server_certificate.public_bytes(serialization.Encoding.PEM),
            server_key=server_key,
            client_certificate=client_certificate.public_bytes(serialization.Encoding.PEM),
            client_key=client_key,
        )

    def secret_body(self, name: str, identity: str, attempt_id: str) -> dict[str, Any]:
        """Build an immutable Secret with the server key and client trust CA."""
        return {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": name,
                "labels": {
                    "app.kubernetes.io/name": "syntara-node",
                    "app.kubernetes.io/managed-by": "execution-plane",
                    "execution-plane.syntara.io/work-item": identity,
                    "execution-plane.syntara.io/attempt": attempt_id,
                },
            },
            "immutable": True,
            "type": "Opaque",
            "data": {
                "server.crt": base64.b64encode(self.server_certificate).decode("ascii"),
                "server.key": base64.b64encode(self.server_key).decode("ascii"),
                "client-ca.crt": base64.b64encode(self.ca_certificate).decode("ascii"),
            },
        }
