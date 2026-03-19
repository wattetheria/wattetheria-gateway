use crate::models::SignedPublicClientSnapshot;
use anyhow::{Context, Result};
use reqwest::Client;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct NodeClient {
    client: Client,
}

impl NodeClient {
    pub fn new(timeout_secs: u64) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(timeout_secs))
            .build()
            .context("build reqwest client")?;
        Ok(Self { client })
    }

    pub async fn fetch_signed_snapshot(
        &self,
        export_url: &str,
    ) -> Result<SignedPublicClientSnapshot> {
        self.client
            .get(export_url)
            .send()
            .await
            .context("request node export")?
            .error_for_status()
            .context("node export returned error status")?
            .json::<SignedPublicClientSnapshot>()
            .await
            .context("parse signed client export")
    }
}
