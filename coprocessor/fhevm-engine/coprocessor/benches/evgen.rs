#[path = "./utils.rs"]
mod utils;
use crate::utils::{
    allow_handle, listener_event_to_db, setup_test_app, wait_until_all_allowed_handles_computed,
    write_to_json, OperatorType,
};
use bigdecimal::num_bigint::BigInt;
use coprocessor::server::common::FheOperation;
use coprocessor::server::coprocessor::{async_computation_input::Input, AsyncComputationInput};
use coprocessor::server::coprocessor::{
    fhevm_coprocessor_client::FhevmCoprocessorClient, AsyncComputation, AsyncComputeRequest,
    InputToUpload, InputUploadBatch,
};
use coprocessor::tfhe_worker;
use criterion::{
    async_executor::FuturesExecutor, measurement::WallTime, Bencher, Criterion, Throughput,
};
use csv::Reader;
use fhevm_engine_common::types::SupportedFheOperations;
use fhevm_engine_common::utils::safe_serialize;
use fhevm_listener::contracts::TfheContract;
use fhevm_listener::contracts::TfheContract::TfheContractEvents;
use fhevm_listener::database::tfhe_event_propagate::{
    ClearConst, Database as ListenerDatabase, Handle, ScalarByte, ToType,
};
use rand::Rng;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use sqlx::Postgres;
use std::future;
use std::ops::{Add, Sub};
use std::time::SystemTime;
use tokio::runtime::Runtime;
use utils::{
    as_scalar_uint, next_random_handle, setup_test_app_existing_db,
    setup_test_app_existing_localhost, tfhe_event, EnvConfig,
};

fn test_random_user_address() -> String {
    let _private_key = "bd2400c676871534a682ca1c5e4cd647ec9c3e122f188c6e3f54e6900d586c7b";
    let public_key = "0x1BdA2a485c339C95a9AbfDe52E80ca38e34C199E";
    public_key.to_string()
}

fn test_random_contract_address() -> String {
    "0x76c222560Db6b8937B291196eAb4Dad8930043aE".to_string()
}

async fn generate_random_handle_amount_if_none(
    result: Option<Handle>,
    transaction_id: Handle,
    listener_event_to_db: &mut ListenerDatabase,
) -> Result<Handle, Box<dyn std::error::Error>> {
    if let Some(res) = result {
        return Ok(res);
    }
    let handle = next_random_handle();
    let caller = "0x0000000000000000000000000000000000000000"
        .parse()
        .unwrap();
    let log = alloy::rpc::types::Log {
        inner: tfhe_event(TfheContractEvents::TrivialEncrypt(
            TfheContract::TrivialEncrypt {
                caller,
                pt: as_scalar_uint(&BigInt::from(rand::rng().random::<u64>())),
                toType: 5u8,
                result: handle,
            },
        )),
        block_hash: None,
        block_number: None,
        block_timestamp: None,
        transaction_hash: Some(transaction_id),
        transaction_index: Some(0),
        log_index: None,
        removed: false,
    };

    listener_event_to_db.insert_tfhe_event(&log).await?;
    Ok(handle)
}
async fn erc20_whitepaper_transaction(
    source: Option<Handle>,
    destination: Option<Handle>,
    amount: Option<Handle>,
    listener_event_to_db: &mut ListenerDatabase,
    pool: &sqlx::Pool<Postgres>,
) -> Result<(Handle, Handle), Box<dyn std::error::Error>> {
    let transaction_id = next_random_handle();
    let source =
        generate_random_handle_amount_if_none(source, transaction_id, listener_event_to_db).await?;
    let destination =
        generate_random_handle_amount_if_none(destination, transaction_id, listener_event_to_db)
            .await?;
    let amount =
        generate_random_handle_amount_if_none(amount, transaction_id, listener_event_to_db).await?;

    let has_enough_funds = next_random_handle();
    let caller = "0x0000000000000000000000000000000000000000"
        .parse()
        .unwrap();
    let log = alloy::rpc::types::Log {
        inner: tfhe_event(TfheContractEvents::FheGe(TfheContract::FheGe {
            caller,
            lhs: source,
            rhs: amount,
            result: has_enough_funds,
            scalarByte: ScalarByte::from(false as u8),
        })),
        block_hash: None,
        block_number: None,
        block_timestamp: None,
        transaction_hash: Some(transaction_id),
        transaction_index: Some(0),
        log_index: None,
        removed: false,
    };
    listener_event_to_db.insert_tfhe_event(&log).await?;

    let new_destination_target = next_random_handle();
    let log = alloy::rpc::types::Log {
        inner: tfhe_event(TfheContractEvents::FheAdd(TfheContract::FheAdd {
            caller,
            lhs: destination,
            rhs: amount,
            result: new_destination_target,
            scalarByte: ScalarByte::from(false as u8),
        })),
        block_hash: None,
        block_number: None,
        block_timestamp: None,
        transaction_hash: Some(transaction_id),
        transaction_index: Some(0),
        log_index: None,
        removed: false,
    };
    listener_event_to_db.insert_tfhe_event(&log).await?;

    let new_destination = next_random_handle();
    let log = alloy::rpc::types::Log {
        inner: tfhe_event(TfheContractEvents::FheIfThenElse(
            TfheContract::FheIfThenElse {
                caller,
                control: has_enough_funds,
                ifTrue: new_destination_target,
                ifFalse: destination,
                result: new_destination,
            },
        )),
        block_hash: None,
        block_number: None,
        block_timestamp: None,
        transaction_hash: Some(transaction_id),
        transaction_index: Some(0),
        log_index: None,
        removed: false,
    };
    allow_handle(&new_destination.to_vec(), pool).await?;
    listener_event_to_db.insert_tfhe_event(&log).await?;

    let new_source_target = next_random_handle();
    let log = alloy::rpc::types::Log {
        inner: tfhe_event(TfheContractEvents::FheSub(TfheContract::FheSub {
            caller,
            lhs: source,
            rhs: amount,
            result: new_source_target,
            scalarByte: ScalarByte::from(false as u8),
        })),
        block_hash: None,
        block_number: None,
        block_timestamp: None,
        transaction_hash: Some(transaction_id),
        transaction_index: Some(0),
        log_index: None,
        removed: false,
    };
    listener_event_to_db.insert_tfhe_event(&log).await?;

    let new_source = next_random_handle();
    let log = alloy::rpc::types::Log {
        inner: tfhe_event(TfheContractEvents::FheIfThenElse(
            TfheContract::FheIfThenElse {
                caller,
                control: has_enough_funds,
                ifTrue: new_source_target,
                ifFalse: source,
                result: new_source,
            },
        )),
        block_hash: None,
        block_number: None,
        block_timestamp: None,
        transaction_hash: Some(transaction_id),
        transaction_index: Some(0),
        log_index: None,
        removed: false,
    };
    allow_handle(&new_source.to_vec(), pool).await?;
    listener_event_to_db.insert_tfhe_event(&log).await?;

    Ok((new_source, new_destination))
}

async fn generate_erc20_at_rate(
    scenario: Vec<(f64, u64)>,
    dependent: bool,
    listener_event_to_db: &mut ListenerDatabase,
    pool: &sqlx::Pool<Postgres>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut dependence_handle = next_random_handle();
    if dependent {
        dependence_handle =
            generate_random_handle_amount_if_none(None, next_random_handle(), listener_event_to_db)
                .await?;
        allow_handle(&dependence_handle.to_vec(), pool).await?;
    }
    for (target_throughput, duration_seconds) in scenario.iter() {
        let start_time = SystemTime::now();
        let mut last_transaction_time = SystemTime::now();
        let end_target = start_time.add(std::time::Duration::from_secs(*duration_seconds));
        let time_between_transactions = std::time::Duration::from_secs_f64(1.0 / target_throughput);

        loop {
            let transaction_start = SystemTime::now();
            if transaction_start > end_target {
                break;
            }
            if dependent {
                (_, dependence_handle) = erc20_whitepaper_transaction(
                    None,
                    Some(dependence_handle),
                    None,
                    listener_event_to_db,
                    pool,
                )
                .await?;
            } else {
                let (_, _) =
                    erc20_whitepaper_transaction(None, None, None, listener_event_to_db, pool)
                        .await?;
            }
            tokio::time::sleep(
                time_between_transactions
                    .sub(SystemTime::now().duration_since(last_transaction_time)?),
            )
            .await;
            last_transaction_time = SystemTime::now();
        }
    }
    Ok(())
}

async fn generate_erc20_count(
    scenario: Vec<(f64, u64)>,
    dependent: bool,
    listener_event_to_db: &mut ListenerDatabase,
    pool: &sqlx::Pool<Postgres>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut dependence_handle = next_random_handle();
    if dependent {
        dependence_handle =
            generate_random_handle_amount_if_none(None, next_random_handle(), listener_event_to_db)
                .await?;
        allow_handle(&dependence_handle.to_vec(), pool).await?;
    }
    for (num_transactions, iter_count) in scenario.iter() {
        for _ in 0..*iter_count {
            for _ in 0..(*num_transactions as u64) {
                if dependent {
                    (_, dependence_handle) = erc20_whitepaper_transaction(
                        None,
                        Some(dependence_handle),
                        None,
                        listener_event_to_db,
                        pool,
                    )
                    .await?;
                } else {
                    let (_, _) =
                        erc20_whitepaper_transaction(None, None, None, listener_event_to_db, pool)
                            .await?;
                }
            }
        }
    }
    Ok(())
}

async fn transaction_generator() -> Result<(), Box<dyn std::error::Error>> {
    let ecfg = EnvConfig::new();
    let app = setup_test_app_existing_db().await?;
    let scenario = [
        (
            vec![(0.5, 300u64), (5.0, 45u64), (15.0, 15u64), (1.0, 240u64)],
            true,
        ),
        (vec![(1.5, 600u64)], false),
        (
            vec![(4.5, 100u64), (1.0, 350u64), (3.0, 100u64), (1.0, 50u64)],
            true,
        ),
        (
            vec![(1.5, 200u64), (2.0, 200u64), (3.0, 100u64), (1.0, 100u64)],
            false,
        ),
    ];
    let scenario_mini = [
        (
            vec![(0.5, 4u64)], // (1.0, 2u64), (2.0, 1u64), (1.0, 1u64)],
            true,
        ),
        // (vec![(0.5, 8u64)], false),
        // (vec![(0.5, 3u64), (1.0, 1u64), (0.2, 4u64)], true),
        // (vec![(0.5, 2u64), (1.0, 6u64)], false),
    ];
    let scenario_maxi = [
        (
            vec![
                (4000.0, 30u64),
                (5000.0, 30u64),
                (1500.0, 10u64),
                (1000.0, 20u64),
            ],
            true,
        ),
        (vec![(5000.0, 90u64)], false),
    ];
    //if ecfg.evgen_scenario != "DEFAULT" {}
    let generators: Vec<_> = scenario_maxi
        .iter()
        .map(|s| {
            let s = s.to_owned();
            tokio::spawn(async move {
                let app = setup_test_app_existing_localhost().await.unwrap();
                let pool = sqlx::postgres::PgPoolOptions::new()
                    .max_connections(2)
                    .connect(app.db_url())
                    .await
                    .unwrap();
                let mut listener_event_to_db = listener_event_to_db(&app).await;

                //if generate_erc20_at_rate(s.0, s.1, &mut listener_event_to_db, &pool)
                if generate_erc20_count(s.0, s.1, &mut listener_event_to_db, &pool)
                    .await
                    .is_err()
                {
                    panic!("Generating ERC20 transactions failed");
                }
            })
        })
        .collect();
    futures::future::join_all(generators).await;

    // if wait_until_all_allowed_handles_computed(app.db_url().to_string())
    //     .await
    //     .is_err()
    // {
    //     panic!("TFHE worker failed");
    // }

    Ok(())
}

fn main() {
    //rayon::join(
    //  || {
    Runtime::new()
        .unwrap()
        .block_on(transaction_generator())
        .unwrap();
    // },
    //     || {
    //         Runtime::new().unwrap().block_on(tfhe_worker()).unwrap();
    //     },
    // );

    println!("Done");
}
