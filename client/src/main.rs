//! This example demonstrates an HTTP client that requests files from a server.
//!
//! Checkout the `README.md` for guidance.
use client::stats_collection::*;
use quinn::ClientConfig;
use {
    client::{
        cli::{build_cli_parameters, ClientCliParameters},
        error::QuicClientError,
        quic_networking::{
            create_client_config, create_client_endpoint, send_data_over_stream,
            QuicClientCertificate,
        },
        transaction_generator::generate_dummy_data,
    },
    solana_sdk::{packet::PACKET_DATA_SIZE, signature::Keypair, signer::EncodableKey},
    std::{
        sync::Arc,
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    },
    tokio::{task::JoinSet, time::sleep},
    tracing::{error, info},
};

fn main() {
    // Check if output is going to a terminal (stdout)
    let is_terminal = atty::is(atty::Stream::Stderr);
    tracing::subscriber::set_global_default(
        tracing_subscriber::FmtSubscriber::builder()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_writer(std::io::stderr)
            .with_ansi(is_terminal)
            .finish(),
    )
    .unwrap();
    let opt = build_cli_parameters();
    let code = {
        if let Err(e) = run(opt) {
            eprintln!("ERROR: {e}");
            1
        } else {
            0
        }
    };
    ::std::process::exit(code);
}

#[tokio::main]
async fn run(parameters: ClientCliParameters) -> Result<(), QuicClientError> {
    let client_certificate =
        if let Some(staked_identity_file) = parameters.staked_identity_file.clone() {
            let staked_identity = Keypair::read_from_file(staked_identity_file)
                .map_err(|_err| QuicClientError::KeypairReadFailure)?;
            Arc::new(QuicClientCertificate::new(&staked_identity))
        } else {
            Arc::new(QuicClientCertificate::default())
        };
    let client_config = create_client_config(client_certificate, parameters.disable_congestion);

    let mut tasks = JoinSet::new();

    for task_id in 0..parameters.num_connections {
        tasks.spawn(run_endpoint(
            client_config.clone(),
            parameters.clone(),
            task_id,
        ));
    }

    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(_) => info!("Task completed successfully"),
            Err(e) => error!("Task failed: {:?}", e),
        }
    }

    Ok(())
}

// quinn has one global per-endpoint lock, so multiple endpoints help get around that
async fn run_endpoint(
    client_config: ClientConfig,
    ClientCliParameters {
        target,
        bind,
        duration,
        tx_size,
        max_txs_num,
        num_connections,
        ..
    }: ClientCliParameters,
    task_id: usize,
) -> Result<(), QuicClientError> {
    let endpoint =
        create_client_endpoint(bind, client_config).expect("Endpoint creation should not fail.");

    let start = Instant::now();

    info!("connecting task `{task_id}` to {target}");
    let connection = endpoint.connect(target, "connect")?.await?;
    info!("connected task `{task_id}` at {:?}", start.elapsed());

    let mut stats_collector: StatsCollection = StatsCollection::new();
    let mut epoch_time: u64 = 0;

    println!("connection.stats():{:?}", connection.stats());

    let start = Instant::now();
    let mut transaction_id = 0;
    let mut tx_buffer = [0u8; PACKET_DATA_SIZE];
    loop {
        let con_stats = connection.stats();
        epoch_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let stats = StatsSample {
            tps: None,
            udp_tx: con_stats.udp_tx.bytes,
            udp_rx: con_stats.udp_rx.bytes,
            time_stamp: epoch_time,
        };
        stats_collector.push(stats);

        if let Some(duration) = duration {
            if start.elapsed() >= duration {
                info!("Transaction generator for task `{task_id}` is stopping...");
                break;
            }
        }
        if let Some(max_txs_num) = max_txs_num {
            if transaction_id == max_txs_num / num_connections {
                info!("Transaction generator for task `{task_id}` is stopping...");
                break;
            }
        }

        generate_dummy_data(&mut tx_buffer, transaction_id, timestamp(), tx_size);
        let _ = send_data_over_stream(&connection, &tx_buffer[0..tx_size as usize]).await;
        transaction_id += 1;
    }

    // When the connection is closed all the streams that haven't been delivered yet will be lost.
    // Sleep to give it some time to deliver all the pending streams.
    sleep(Duration::from_secs(1)).await;
    let connection_stats = connection.stats();
    println!("connection.stats():{:?}", connection_stats);
    info!("Connection stats for task `{task_id}`: {connection_stats:?}");
    connection.close(0u32.into(), b"done");

    // Give the server a fair chance to receive the close packet
    endpoint.wait_idle().await;
    Ok(())
}

/// return timestamp as ms
pub fn timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("create timestamp in timing")
        .as_millis() as u64
}
