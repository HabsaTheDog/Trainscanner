#[cfg(feature = "temporal-worker")]
mod activities;

#[cfg(feature = "temporal-worker")]
mod runtime {
    use anyhow::Result;
    use std::env;
    use std::str::FromStr;
    use std::sync::Arc;
    use temporal_sdk::prelude::worker::{
        init_worker, sdk_client_options, CoreRuntime, TelemetryOptionsBuilder, Url, Worker,
        WorkerConfigBuilder, WorkerVersioningStrategy,
    };

    use crate::activities::netex::extract_netex_stops;

    #[tokio::main]
    pub async fn main() -> Result<()> {
        env_logger::init();

        let temporal_address =
            env::var("TEMPORAL_ADDRESS").unwrap_or_else(|_| "http://localhost:7233".to_owned());
        let temporal_namespace =
            env::var("TEMPORAL_NAMESPACE").unwrap_or_else(|_| "default".to_owned());
        let task_queue =
            env::var("TEMPORAL_TASK_QUEUE").unwrap_or_else(|_| "review-pipeline".to_owned());

        let server_options = sdk_client_options(Url::from_str(&temporal_address)?).build()?;
        let client = server_options
            .connect(temporal_namespace.clone(), None)
            .await?;

        let telemetry_options = TelemetryOptionsBuilder::default().build()?;
        let runtime = CoreRuntime::new_assume_tokio(telemetry_options)?;
        let worker_config = WorkerConfigBuilder::default()
            .namespace(temporal_namespace)
            .task_queue(task_queue.clone())
            .versioning_strategy(WorkerVersioningStrategy::None {
                build_id: "rust-ingestion-worker".to_owned(),
            })
            .build()?;
        let core_worker = init_worker(&runtime, worker_config, client)?;

        let mut worker = Worker::new_from_core(Arc::new(core_worker), task_queue.clone());
        worker.register_activity("extract_netex_stops", extract_netex_stops);

        log::info!("Starting Rust ingestion worker on queue '{}'", task_queue);
        worker.run().await?;
        Ok(())
    }
}

#[cfg(feature = "temporal-worker")]
fn main() -> anyhow::Result<()> {
    runtime::main()
}

#[cfg(not(feature = "temporal-worker"))]
fn main() {
    eprintln!(
        "rust-ingestion built without 'temporal-worker' feature; enable with: cargo run --features temporal-worker --manifest-path services/rust-ingestion-worker/Cargo.toml"
    );
}
