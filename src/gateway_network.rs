use crate::config::GatewayP2pConfig;
use anyhow::Result;
use serde::Serialize;
use wattswarm_network_substrate::{
    NetworkRuntimeObservabilitySnapshot, PeerId, SubstrateNode, SubstrateRuntime, SwarmScope,
    TrafficGuardPeerHealth,
};
use wattswarm_network_transport_core::{
    DirectDataTransportAdapter, PeerTransportCapabilities, TransferIntent,
    TransportContactMaterial, TransportRoute, TransportRouter,
};
use wattswarm_network_transport_iroh::IrohTransportAdapter;

pub struct GatewayNetworkNode {
    inner: SubstrateNode,
}

impl GatewayNetworkNode {
    pub fn generate(config: GatewayP2pConfig) -> Result<Self> {
        Ok(Self {
            inner: SubstrateNode::generate(config.as_substrate())?,
        })
    }
}

pub struct GatewayNetworkRuntime {
    inner: SubstrateRuntime,
    iroh_adapter: IrohTransportAdapter,
}

#[derive(Debug, Clone, Serialize)]
pub struct GatewayNetworkInfo {
    pub peer_id: String,
    pub listen_addrs: Vec<String>,
    pub transport_capabilities: PeerTransportCapabilities,
    pub transport_contact_material: Option<TransportContactMaterial>,
    pub nat_status: String,
    pub nat_public_address: Option<String>,
    pub nat_confidence: u32,
    pub relay_reservations: Vec<String>,
    pub peer_health: Vec<GatewayPeerHealth>,
}

#[derive(Debug, Clone, Serialize)]
pub struct GatewayPeerHealth {
    pub peer: String,
    pub score: i64,
    pub blacklisted: bool,
    pub reputation_tier: String,
    pub quarantined: bool,
    pub quarantine_remaining_ms: u64,
    pub ban_remaining_ms: u64,
    pub throttle_factor_percent: u32,
}

impl From<TrafficGuardPeerHealth> for GatewayPeerHealth {
    fn from(value: TrafficGuardPeerHealth) -> Self {
        Self {
            peer: value.peer,
            score: value.score,
            blacklisted: value.blacklisted,
            reputation_tier: value.reputation_tier,
            quarantined: value.quarantined,
            quarantine_remaining_ms: value.quarantine_remaining_ms,
            ban_remaining_ms: value.ban_remaining_ms,
            throttle_factor_percent: value.throttle_factor_percent,
        }
    }
}

impl GatewayNetworkRuntime {
    pub fn new(node: GatewayNetworkNode) -> Result<Self> {
        let local_peer_id = node.inner.local_peer_id();
        let mut inner = SubstrateRuntime::new(node.inner)?;
        inner.subscribe_scope(&SwarmScope::Global)?;
        Ok(Self {
            inner,
            iroh_adapter: IrohTransportAdapter::from_local_peer_id(&local_peer_id)?,
        })
    }

    pub fn local_peer_id(&self) -> PeerId {
        self.inner.local_peer_id()
    }

    pub fn listen_addrs(&self) -> Vec<String> {
        self.inner
            .listen_addrs()
            .iter()
            .map(ToString::to_string)
            .collect()
    }

    pub fn observability_snapshot(&self) -> NetworkRuntimeObservabilitySnapshot {
        self.inner.observability_snapshot()
    }

    pub fn transport_capabilities(&self) -> PeerTransportCapabilities {
        self.iroh_adapter.capabilities().clone()
    }

    pub fn export_transport_contact_material(
        &self,
        generated_at: u64,
    ) -> Result<TransportContactMaterial> {
        Ok(self
            .iroh_adapter
            .export_contact_material(&self.listen_addrs(), generated_at)?)
    }

    pub fn recommended_transfer_route(
        &self,
        remote_capabilities: Option<&PeerTransportCapabilities>,
        intent: &TransferIntent,
    ) -> TransportRoute {
        TransportRouter::select(intent, remote_capabilities)
    }

    pub fn export_info(&self, generated_at: u64) -> Result<GatewayNetworkInfo> {
        let snapshot: NetworkRuntimeObservabilitySnapshot = self.observability_snapshot();
        Ok(GatewayNetworkInfo {
            peer_id: self.local_peer_id().to_string(),
            listen_addrs: self.listen_addrs(),
            transport_capabilities: self.transport_capabilities(),
            transport_contact_material: Some(self.export_transport_contact_material(generated_at)?),
            nat_status: snapshot.nat_status,
            nat_public_address: snapshot.nat_public_address,
            nat_confidence: snapshot.nat_confidence,
            relay_reservations: snapshot.relay_reservations,
            peer_health: snapshot.peer_health.into_iter().map(Into::into).collect(),
        })
    }
}
