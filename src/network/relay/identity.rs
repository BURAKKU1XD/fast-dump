use std::sync::{Arc, OnceLock};

use anyhow::{Context, Result};
use ring::digest::{SHA256, digest};
use ring::rand::SystemRandom;
use ring::signature::Ed25519KeyPair;
use subtle::ConstantTimeEq;
use tokio_rustls::rustls::client::danger::{
    HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
};
use tokio_rustls::rustls::crypto::{ring as provider, verify_tls13_signature_with_raw_key};
use tokio_rustls::rustls::pki_types::{
    CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, SubjectPublicKeyInfoDer,
    UnixTime,
};
use tokio_rustls::rustls::server::AlwaysResolvesServerRawPublicKeys;
use tokio_rustls::rustls::sign::CertifiedKey;
use tokio_rustls::rustls::{
    CertificateError, ClientConfig, DigitallySignedStruct, Error, ServerConfig, SignatureScheme,
    version::TLS13,
};
use tokio_rustls::{TlsAcceptor, TlsConnector};

// DER SubjectPublicKeyInfo для Ed25519 по RFC 8410 занимает 44 байта.
pub(super) type PublicKey = [u8; 44];

#[derive(Debug)]
pub(super) enum PinnedKey {
    FirstUse(OnceLock<PublicKey>),
    PublicKey(PublicKey),
    Fingerprint([u8; 32]),
}

pub(super) fn generate() -> Result<(TlsAcceptor, PublicKey)> {
    let key = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())
        .map_err(|_| anyhow::anyhow!("generate relay TLS identity"))?;
    let private = PrivateKeyDer::from(PrivatePkcs8KeyDer::from(key.as_ref()));
    let signing_key = provider::sign::any_supported_type(&private)?;
    let public = signing_key.public_key().context("get relay public key")?;
    let public_key = public
        .as_ref()
        .try_into()
        .context("invalid relay public key length")?;
    let key = CertifiedKey::new(
        vec![CertificateDer::from(public.as_ref().to_vec())],
        signing_key,
    );
    let resolver = AlwaysResolvesServerRawPublicKeys::new(Arc::new(key));
    let config = ServerConfig::builder_with_protocol_versions(&[&TLS13])
        .with_no_client_auth()
        .with_cert_resolver(Arc::new(resolver));
    Ok((TlsAcceptor::from(Arc::new(config)), public_key))
}

pub(super) fn connector(expected: PinnedKey) -> TlsConnector {
    let config = ClientConfig::builder_with_protocol_versions(&[&TLS13])
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(expected))
        .with_no_client_auth();
    TlsConnector::from(Arc::new(config))
}

pub(super) fn fingerprint(public: &[u8]) -> [u8; 32] {
    digest(&SHA256, public).as_ref().try_into().unwrap()
}

impl ServerCertVerifier for PinnedKey {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _: &ServerName<'_>,
        _: &[u8],
        _: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        let public = end_entity.as_ref();
        let matches = match self {
            Self::FirstUse(first) => {
                // Один коннектор хранит ключ лобби и проверяет его на каналах передачи.
                <&PublicKey>::try_from(public)
                    .is_ok_and(|key| bool::from(key.ct_eq(first.get_or_init(|| *key))))
            }
            Self::PublicKey(key) => bool::from(public.ct_eq(key)),
            Self::Fingerprint(expected) => bool::from(fingerprint(public).ct_eq(expected)),
        };
        if intermediates.is_empty() && matches {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(Error::InvalidCertificate(
                CertificateError::ApplicationVerificationFailure,
            ))
        }
    }

    fn verify_tls12_signature(
        &self,
        _: &[u8],
        _: &CertificateDer<'_>,
        _: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        Err(Error::General(
            "relay raw public keys require TLS 1.3".into(),
        ))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        verify_tls13_signature_with_raw_key(
            message,
            &SubjectPublicKeyInfoDer::from(cert.as_ref()),
            signature,
            &provider::default_provider().signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![SignatureScheme::ED25519]
    }

    fn requires_raw_public_keys(&self) -> bool {
        true
    }
}
