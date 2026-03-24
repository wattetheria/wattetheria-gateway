use crate::models::{
    GatewayRegistryDbRow, GatewayRegistryEntry, NodeSourceRow, SignedGatewayManifest, SnapshotRow,
};
use anyhow::Result;
use chrono::Utc;
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use uuid::Uuid;

pub struct UpsertSnapshotRecord<'a> {
    pub source_id: Option<Uuid>,
    pub node_id: &'a str,
    pub signer_agent_did: &'a str,
    pub public_key: &'a str,
    pub generated_at: i64,
    pub payload: &'a Value,
    pub signature: &'a str,
}

pub struct UpsertGatewayManifestRecord<'a> {
    pub manifest: &'a SignedGatewayManifest,
}

pub async fn connect(database_url: &str) -> Result<PgPool> {
    Ok(PgPoolOptions::new()
        .max_connections(10)
        .connect(database_url)
        .await?)
}

pub async fn init_schema(pool: &PgPool) -> Result<()> {
    sqlx::query(
        r#"
        create table if not exists node_sources (
            id uuid primary key,
            name text not null,
            export_url text not null unique,
            region text null,
            expected_signer_agent_did text null,
            created_at timestamptz not null default now(),
            updated_at timestamptz not null default now(),
            last_sync_at timestamptz null,
            last_sync_status text null,
            last_error text null
        );
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        create table if not exists node_snapshots (
            node_id text primary key,
            source_id uuid null references node_sources(id) on delete set null,
            signer_agent_did text not null,
            public_key text not null,
            generated_at bigint not null,
            ingested_at timestamptz not null default now(),
            payload jsonb not null,
            signature text not null
        );
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        create index if not exists idx_node_snapshots_node_id on node_snapshots(node_id);
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        create table if not exists gateway_registry_entries (
            gateway_id text primary key,
            display_name text not null,
            base_url text not null unique,
            public_key text not null,
            region text null,
            operator_did text null,
            roles jsonb not null default '[]'::jsonb,
            supported_endpoints jsonb not null default '[]'::jsonb,
            federation_peers jsonb not null default '[]'::jsonb,
            allows_public_ingest boolean not null default false,
            manifest_payload jsonb not null,
            manifest_signature text not null,
            status text not null default 'pending',
            discovery_tier text not null default 'community',
            review_reason text null,
            reviewed_at timestamptz null,
            reviewed_by text null,
            created_at timestamptz not null default now(),
            updated_at timestamptz not null default now()
        );
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        do $$
        begin
            if exists (
                select 1
                from information_schema.columns
                where table_name = 'node_sources' and column_name = 'expected_signer_agent_id'
            ) and not exists (
                select 1
                from information_schema.columns
                where table_name = 'node_sources' and column_name = 'expected_signer_agent_did'
            ) then
                alter table node_sources
                rename column expected_signer_agent_id to expected_signer_agent_did;
            end if;
        end
        $$;
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        do $$
        begin
            if exists (
                select 1
                from information_schema.columns
                where table_name = 'node_snapshots' and column_name = 'signer_agent_id'
            ) and not exists (
                select 1
                from information_schema.columns
                where table_name = 'node_snapshots' and column_name = 'signer_agent_did'
            ) then
                alter table node_snapshots
                rename column signer_agent_id to signer_agent_did;
            end if;
        end
        $$;
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        do $$
        begin
            if exists (
                select 1
                from information_schema.columns
                where table_name = 'gateway_registry_entries' and column_name = 'operator_id'
            ) and not exists (
                select 1
                from information_schema.columns
                where table_name = 'gateway_registry_entries' and column_name = 'operator_did'
            ) then
                alter table gateway_registry_entries
                rename column operator_id to operator_did;
            end if;
        end
        $$;
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        create index if not exists idx_gateway_registry_entries_status on gateway_registry_entries(status);
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        create index if not exists idx_gateway_registry_entries_tier on gateway_registry_entries(discovery_tier);
        "#,
    )
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn insert_node_source(
    pool: &PgPool,
    id: Uuid,
    name: &str,
    export_url: &str,
    region: Option<&str>,
    expected_signer_agent_did: Option<&str>,
) -> Result<()> {
    sqlx::query(
        r#"
        insert into node_sources (
            id, name, export_url, region, expected_signer_agent_did, created_at, updated_at
        )
        values ($1, $2, $3, $4, $5, now(), now())
        "#,
    )
    .bind(id)
    .bind(name)
    .bind(export_url)
    .bind(region)
    .bind(expected_signer_agent_did)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn list_node_sources(pool: &PgPool) -> Result<Vec<NodeSourceRow>> {
    Ok(sqlx::query_as::<_, NodeSourceRow>(
        r#"
        select
            id,
            name,
            export_url,
            region,
            expected_signer_agent_did,
            created_at,
            updated_at,
            last_sync_at,
            last_sync_status,
            last_error
        from node_sources
        order by created_at asc
        "#,
    )
    .fetch_all(pool)
    .await?)
}

pub async fn get_node_source(pool: &PgPool, source_id: Uuid) -> Result<Option<NodeSourceRow>> {
    Ok(sqlx::query_as::<_, NodeSourceRow>(
        r#"
        select
            id,
            name,
            export_url,
            region,
            expected_signer_agent_did,
            created_at,
            updated_at,
            last_sync_at,
            last_sync_status,
            last_error
        from node_sources
        where id = $1
        "#,
    )
    .bind(source_id)
    .fetch_optional(pool)
    .await?)
}

pub async fn update_source_sync_status(
    pool: &PgPool,
    source_id: Uuid,
    status: &str,
    error: Option<&str>,
) -> Result<()> {
    sqlx::query(
        r#"
        update node_sources
        set
            updated_at = now(),
            last_sync_at = now(),
            last_sync_status = $2,
            last_error = $3
        where id = $1
        "#,
    )
    .bind(source_id)
    .bind(status)
    .bind(error)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn upsert_snapshot(pool: &PgPool, record: UpsertSnapshotRecord<'_>) -> Result<()> {
    sqlx::query(
        r#"
        insert into node_snapshots (
            node_id, source_id, signer_agent_did, public_key, generated_at, ingested_at, payload, signature
        )
        values ($1, $2, $3, $4, $5, now(), $6, $7)
        on conflict (node_id) do update
        set
            source_id = coalesce(excluded.source_id, node_snapshots.source_id),
            signer_agent_did = excluded.signer_agent_did,
            public_key = excluded.public_key,
            generated_at = excluded.generated_at,
            ingested_at = now(),
            payload = excluded.payload,
            signature = excluded.signature
        where excluded.generated_at >= node_snapshots.generated_at
        "#,
    )
    .bind(record.node_id)
    .bind(record.source_id)
    .bind(record.signer_agent_did)
    .bind(record.public_key)
    .bind(record.generated_at)
    .bind(sqlx::types::Json(record.payload))
    .bind(record.signature)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn list_snapshots(pool: &PgPool) -> Result<Vec<SnapshotRow>> {
    Ok(sqlx::query_as::<_, SnapshotRow>(
        r#"
        select
            source_id,
            node_id,
            signer_agent_did,
            public_key,
            generated_at,
            ingested_at,
            payload,
            signature
        from node_snapshots
        order by ingested_at desc
        "#,
    )
    .fetch_all(pool)
    .await?)
}

pub async fn counts(pool: &PgPool) -> Result<(i64, i64)> {
    let row = sqlx::query(
        r#"
        select
            (select count(*) from node_sources) as source_count,
            (select count(*) from node_snapshots) as snapshot_count
        "#,
    )
    .fetch_one(pool)
    .await?;
    Ok((row.try_get("source_count")?, row.try_get("snapshot_count")?))
}

pub async fn upsert_gateway_manifest(
    pool: &PgPool,
    record: UpsertGatewayManifestRecord<'_>,
) -> Result<GatewayRegistryEntry> {
    let payload = &record.manifest.payload;
    sqlx::query(
        r#"
        insert into gateway_registry_entries (
            gateway_id,
            display_name,
            base_url,
            public_key,
            region,
            operator_did,
            roles,
            supported_endpoints,
            federation_peers,
            allows_public_ingest,
            manifest_payload,
            manifest_signature,
            status,
            discovery_tier,
            created_at,
            updated_at
        )
        values (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 'pending', 'community', now(), now()
        )
        on conflict (gateway_id) do update
        set
            display_name = excluded.display_name,
            base_url = excluded.base_url,
            public_key = excluded.public_key,
            region = excluded.region,
            operator_did = excluded.operator_did,
            roles = excluded.roles,
            supported_endpoints = excluded.supported_endpoints,
            federation_peers = excluded.federation_peers,
            allows_public_ingest = excluded.allows_public_ingest,
            manifest_payload = excluded.manifest_payload,
            manifest_signature = excluded.manifest_signature,
            updated_at = now()
        "#,
    )
    .bind(&payload.gateway_id)
    .bind(&payload.display_name)
    .bind(&payload.base_url)
    .bind(&payload.public_key)
    .bind(payload.region.as_deref())
    .bind(payload.operator_did.as_deref())
    .bind(sqlx::types::Json(&payload.roles))
    .bind(sqlx::types::Json(&payload.supported_endpoints))
    .bind(sqlx::types::Json(&payload.federation_peers))
    .bind(payload.allows_public_ingest)
    .bind(sqlx::types::Json(payload))
    .bind(&record.manifest.signature)
    .execute(pool)
    .await?;

    get_gateway_registry_entry(pool, &payload.gateway_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("registered gateway entry missing after upsert"))
}

pub async fn review_gateway_manifest(
    pool: &PgPool,
    gateway_id: &str,
    status: &str,
    discovery_tier: &str,
    reason: Option<&str>,
    reviewed_by: Option<&str>,
) -> Result<Option<GatewayRegistryEntry>> {
    sqlx::query(
        r#"
        update gateway_registry_entries
        set
            status = $2,
            discovery_tier = $3,
            review_reason = $4,
            reviewed_by = $5,
            reviewed_at = now(),
            updated_at = now()
        where gateway_id = $1
        "#,
    )
    .bind(gateway_id)
    .bind(status)
    .bind(discovery_tier)
    .bind(reason)
    .bind(reviewed_by)
    .execute(pool)
    .await?;
    get_gateway_registry_entry(pool, gateway_id).await
}

pub async fn list_gateway_registry_entries(
    pool: &PgPool,
    status: Option<&str>,
    tier: Option<&str>,
    region: Option<&str>,
    role: Option<&str>,
) -> Result<Vec<GatewayRegistryEntry>> {
    let rows = sqlx::query_as::<_, GatewayRegistryDbRow>(
        r#"
        select
            gateway_id,
            display_name,
            base_url,
            public_key,
            region,
            operator_did,
            roles,
            supported_endpoints,
            federation_peers,
            allows_public_ingest,
            manifest_payload,
            manifest_signature,
            status,
            discovery_tier,
            review_reason,
            reviewed_at,
            reviewed_by,
            created_at,
            updated_at
        from gateway_registry_entries
        where ($1::text is null or status = $1)
          and ($2::text is null or discovery_tier = $2)
          and ($3::text is null or region = $3)
          and ($4::text is null or roles ? $4)
        order by
            case status when 'approved' then 0 else 1 end,
            case discovery_tier
                when 'official' then 0
                when 'verified' then 1
                when 'community' then 2
                when 'manual' then 3
                else 4
            end,
            updated_at desc
        "#,
    )
    .bind(status)
    .bind(tier)
    .bind(region)
    .bind(role)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(Into::into).collect())
}

pub async fn get_gateway_registry_entry(
    pool: &PgPool,
    gateway_id: &str,
) -> Result<Option<GatewayRegistryEntry>> {
    let row = sqlx::query_as::<_, GatewayRegistryDbRow>(
        r#"
        select
            gateway_id,
            display_name,
            base_url,
            public_key,
            region,
            operator_did,
            roles,
            supported_endpoints,
            federation_peers,
            allows_public_ingest,
            manifest_payload,
            manifest_signature,
            status,
            discovery_tier,
            review_reason,
            reviewed_at,
            reviewed_by,
            created_at,
            updated_at
        from gateway_registry_entries
        where gateway_id = $1
        "#,
    )
    .bind(gateway_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(Into::into))
}

pub fn now_rfc3339() -> String {
    Utc::now().to_rfc3339()
}
