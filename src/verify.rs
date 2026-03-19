use crate::models::{SignedGatewayManifest, SignedPublicClientSnapshot};
use anyhow::{Context, Result, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use ed25519_dalek::{Signature, Verifier, VerifyingKey};

pub fn canonical_bytes(payload: &impl serde::Serialize) -> Result<Vec<u8>> {
    let json = serde_jcs::to_string(payload).context("canonicalize payload")?;
    Ok(json.into_bytes())
}

pub fn verify_signed_snapshot(
    snapshot: &SignedPublicClientSnapshot,
    expected_signer_agent_id: Option<&str>,
) -> Result<()> {
    let payload = &snapshot.payload;
    if payload.node_id.trim().is_empty() {
        bail!("snapshot node_id is empty");
    }
    if payload.public_key.trim().is_empty() {
        bail!("snapshot public_key is empty");
    }
    if let Some(expected) = expected_signer_agent_id
        && snapshot.signer_agent_id != expected
    {
        bail!(
            "unexpected signer_agent_id: expected {expected}, got {}",
            snapshot.signer_agent_id
        );
    }
    if snapshot.signer_agent_id != payload.public_key {
        bail!("signer_agent_id does not match payload public_key");
    }
    verify_canonical_signature(
        payload,
        &snapshot.signature,
        &payload.public_key,
        "snapshot signature",
    )?;
    Ok(())
}

pub fn verify_signed_gateway_manifest(manifest: &SignedGatewayManifest) -> Result<()> {
    let payload = &manifest.payload;
    if payload.gateway_id.trim().is_empty() {
        bail!("manifest gateway_id is empty");
    }
    if payload.display_name.trim().is_empty() {
        bail!("manifest display_name is empty");
    }
    if payload.base_url.trim().is_empty() {
        bail!("manifest base_url is empty");
    }
    if payload.public_key.trim().is_empty() {
        bail!("manifest public_key is empty");
    }
    if payload.roles.is_empty() {
        bail!("manifest roles must not be empty");
    }
    verify_canonical_signature(
        payload,
        &manifest.signature,
        &payload.public_key,
        "gateway manifest signature",
    )?;
    Ok(())
}

pub fn verify_canonical_signature(
    payload: &impl serde::Serialize,
    signature_b64: &str,
    public_key_b64: &str,
    context_label: &str,
) -> Result<()> {
    let public = STANDARD
        .decode(public_key_b64)
        .context("decode public key base64")?;
    let signature = STANDARD
        .decode(signature_b64)
        .context("decode signature base64")?;
    let verifying = VerifyingKey::from_bytes(
        public
            .as_slice()
            .try_into()
            .map_err(|_| anyhow::anyhow!("invalid public key length"))?,
    )
    .context("decode ed25519 verifying key")?;
    let sig = Signature::from_bytes(
        signature
            .as_slice()
            .try_into()
            .map_err(|_| anyhow::anyhow!("invalid signature length"))?,
    );
    verifying
        .verify(&canonical_bytes(payload)?, &sig)
        .with_context(|| format!("verify {context_label}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{GatewayManifest, PublicClientSnapshot};
    use base64::engine::general_purpose::STANDARD;
    use ed25519_dalek::{Signer, SigningKey};
    use serde_json::json;

    #[test]
    fn signed_snapshot_verifies() {
        let signing_key = SigningKey::from_bytes(&[7_u8; 32]);
        let verifying_key = signing_key.verifying_key();
        let public_key = STANDARD.encode(verifying_key.as_bytes());
        let payload = PublicClientSnapshot {
            generated_at: 1_710_000_000,
            node_id: "node-alpha".to_string(),
            public_key: public_key.clone(),
            network_status: json!({"total_nodes": 2, "active_nodes": 2, "health_percent": 100, "avg_latency_ms": 0}),
            peers: vec![json!({"id":"peer-1"})],
            operator: json!({"id":"agent-root","display_name":"Agent Root"}),
            rpc_logs: vec![],
            public_topics: vec![],
            public_topic_messages: vec![],
            tasks: vec![],
            organizations: vec![],
            leaderboard: vec![],
        };
        let signature = STANDARD.encode(
            signing_key
                .sign(&canonical_bytes(&payload).unwrap())
                .to_bytes(),
        );
        let snapshot = SignedPublicClientSnapshot {
            payload,
            signature,
            signer_agent_id: public_key.clone(),
        };

        verify_signed_snapshot(&snapshot, Some(&public_key)).unwrap();
    }

    #[test]
    fn signed_gateway_manifest_verifies() {
        let signing_key = SigningKey::from_bytes(&[9_u8; 32]);
        let public_key = STANDARD.encode(signing_key.verifying_key().as_bytes());
        let payload = GatewayManifest {
            generated_at: 1_710_000_000,
            gateway_id: "gw-ap-1".to_string(),
            display_name: "AP Gateway".to_string(),
            base_url: "https://gw-ap.example".to_string(),
            public_key: public_key.clone(),
            region: Some("ap-southeast".to_string()),
            operator_id: Some("operator-ap".to_string()),
            roles: vec!["ingest".to_string(), "query".to_string()],
            supported_endpoints: vec!["/api/network/status".to_string()],
            federation_peers: vec!["https://gw-us.example".to_string()],
            allows_public_ingest: true,
        };
        let signature = STANDARD.encode(
            signing_key
                .sign(&canonical_bytes(&payload).unwrap())
                .to_bytes(),
        );
        let manifest = SignedGatewayManifest { payload, signature };

        verify_signed_gateway_manifest(&manifest).unwrap();
    }
}
