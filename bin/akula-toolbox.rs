#![allow(clippy::type_complexity)]

use akula::{
    hex_to_bytes,
    kv::{
        tables::{self, ErasedTable},
        traits::{MutableKV, KV},
        Table, TableDecode, TableEncode,
    },
    models::*,
    stagedsync::{
        self,
        stage::{ExecOutput, Stage, StageInput, UnwindInput},
    },
    stages::{BlockHashes, Execution, SenderRecovery},
    Cursor, MutableCursor, MutableTransaction, StageId, Transaction,
};
use anyhow::{bail, ensure, Context};
use async_trait::async_trait;
use bytes::Bytes;
use ethereum_types::H256;
use itertools::Itertools;
use rayon::prelude::*;
use std::{
    borrow::Cow,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use structopt::StructOpt;
use tokio_stream::StreamExt;
use tracing::*;
use tracing_subscriber::{prelude::*, EnvFilter};

#[derive(StructOpt)]
#[structopt(name = "Akula Toolbox", about = "Utilities for Akula Ethereum client")]
pub enum Opt {
    /// Print database statistics
    DbStats {
        /// Chain data path
        #[structopt(parse(from_os_str))]
        chaindata: PathBuf,
        /// Whether to print CSV
        #[structopt(long)]
        csv: bool,
    },

    /// Query database
    DbQuery {
        #[structopt(long, parse(from_os_str))]
        chaindata: PathBuf,
        #[structopt(long)]
        table: String,
        #[structopt(long, parse(try_from_str = hex_to_bytes))]
        key: Bytes,
    },

    /// Walk over table entries
    DbWalk {
        #[structopt(long, parse(from_os_str))]
        chaindata: PathBuf,
        #[structopt(long)]
        table: String,
        #[structopt(long, parse(try_from_str = hex_to_bytes))]
        starting_key: Option<Bytes>,
        #[structopt(long)]
        max_entries: Option<usize>,
    },

    /// Check table equality in two databases
    CheckEqual {
        #[structopt(long, parse(from_os_str))]
        db1: PathBuf,
        #[structopt(long, parse(from_os_str))]
        db2: PathBuf,
        #[structopt(long)]
        table: String,
    },

    /// Execute Block Hashes stage
    Blockhashes {
        #[structopt(parse(from_os_str))]
        chaindata: PathBuf,
    },

    /// Convert Erigon database and execute it
    ExecuteWithErigon {
        #[structopt(long, parse(from_os_str))]
        erigon_chaindata: PathBuf,

        #[structopt(long, parse(from_os_str))]
        chaindata: PathBuf,
    },
}

macro_rules! convert_stage {
    ($name:ident, $body:expr) => {
        #[derive(Debug)]
        struct $name<Source>
        where
            Source: KV,
        {
            db: Arc<Source>,
        }

        #[async_trait]
        impl<'db, RwTx, Source> Stage<'db, RwTx> for $name<Source>
        where
            Source: KV,
            RwTx: MutableTransaction<'db>,
        {
            fn id(&self) -> StageId {
                StageId(stringify!($name))
            }
            fn description(&self) -> &'static str {
                ""
            }
            #[allow(clippy::redundant_closure_call)]
            async fn execute<'tx>(
                &self,
                tx: &'tx mut RwTx,
                input: StageInput,
            ) -> anyhow::Result<ExecOutput>
            where
                'db: 'tx,
            {
                ($body)(self.db.clone(), tx, input).await
            }
            async fn unwind<'tx>(&self, _: &'tx mut RwTx, _: UnwindInput) -> anyhow::Result<()>
            where
                'db: 'tx,
            {
                todo!()
            }
        }
    };
}

async fn convert_table<'db, 'tx, RwTx, Source, T>(
    db: Arc<Source>,
    tx: &'tx mut RwTx,
    table: T,
    key_to_block_num: impl Fn(&T::Key) -> Option<BlockNumber> + Send + Sync + Copy,
    value_decoder: impl Fn(Vec<u8>) -> anyhow::Result<T::Value> + Send + Sync + Copy,
    value_encoder: impl Fn(<T::Value as TableEncode>::Encoded) -> Vec<u8> + Send + Sync + Copy,
    input: StageInput,
) -> anyhow::Result<ExecOutput>
where
    'db: 'tx,
    RwTx: MutableTransaction<'db>,
    T: Table + Copy,
    T::Key: TableDecode,
    Source: KV,
{
    const BUFFERING_FACTOR: usize = 500_000;
    let erigon_tx = db.begin().await?;
    let mut erigon_cur = erigon_tx.cursor(&ErasedTable(table)).await?;
    let mut cur = tx.mutable_cursor(&ErasedTable(table)).await?;

    let mut highest_block = input.stage_progress;
    let mut walker = erigon_cur.walk(Some(
        TableEncode::encode(input.stage_progress.unwrap_or(BlockNumber(0)).0 + 1).to_vec(),
    ));
    let mut converted = Vec::with_capacity(BUFFERING_FACTOR);
    let mut i = 0;
    loop {
        let mut extracted = Vec::with_capacity(BUFFERING_FACTOR);
        while let Some((k, v)) = walker.try_next().await? {
            extracted.push((k, v));

            i += 1;
            if i % 500_000 == 0 {
                info!("Extracted {} entries", i);
                break;
            }
        }

        if extracted.is_empty() {
            break;
        }

        extracted
            .into_par_iter()
            .map(move |(k, v)| {
                let key = ErasedTable::<T>::decode_key(&k)?;
                let block_num = (key_to_block_num)(&key);
                let value = (value_encoder)((value_decoder)(v)?.encode());
                Ok::<_, anyhow::Error>((block_num, k, value))
            })
            .collect_into_vec(&mut converted);

        for res in converted.drain(..) {
            let (block_num, key, value) = res?;
            if let Some(block_num) = block_num {
                highest_block = Some(block_num);
            }
            cur.append((key, value)).await?;
        }
    }

    Ok(ExecOutput::Progress {
        stage_progress: highest_block.unwrap_or_else(|| {
            input
                .previous_stage
                .map(|(_, v)| v)
                .unwrap_or(BlockNumber(0))
        }),
        done: true,
        must_commit: true,
    })
}

convert_stage!(ConvertHeaders, move |db, tx, input| async move {
    convert_table(
        db,
        tx,
        akula::kv::tables::Header,
        |(block_num, _)| Some(*block_num),
        |v| Ok(rlp::decode(&v)?),
        |b| b.to_vec(),
        input,
    )
    .await
});

convert_stage!(ConvertCanonical, move |db, tx, input| async move {
    convert_table(
        db,
        tx,
        akula::kv::tables::CanonicalHeader,
        |block_num| Some(*block_num),
        |v| H256::decode(&v),
        |b| b.to_vec(),
        input,
    )
    .await
});

convert_stage!(ConvertHeadersTD, move |db, tx, input| async move {
    convert_table(
        db,
        tx,
        akula::kv::tables::HeadersTotalDifficulty,
        |(block_num, _)| Some(*block_num),
        |v| Ok(rlp::decode(&v)?),
        |b| b.to_vec(),
        input,
    )
    .await
});

#[derive(Debug)]
struct ConvertBodies<Source>
where
    Source: KV,
{
    db: Arc<Source>,
}

#[async_trait]
impl<'db, RwTx, Source> Stage<'db, RwTx> for ConvertBodies<Source>
where
    Source: KV,
    RwTx: MutableTransaction<'db>,
{
    fn id(&self) -> StageId {
        StageId("ConvertBodies")
    }
    fn description(&self) -> &'static str {
        ""
    }
    #[allow(clippy::redundant_closure_call)]
    async fn execute<'tx>(&self, tx: &'tx mut RwTx, input: StageInput) -> anyhow::Result<ExecOutput>
    where
        'db: 'tx,
    {
        const BUFFERING_FACTOR: usize = 500_000;
        let erigon_tx = self.db.begin().await?;
        let mut erigon_body_cur = erigon_tx.cursor(&tables::BlockBody.erased()).await?;
        let mut body_cur = tx.mutable_cursor(&tables::BlockBody.erased()).await?;

        let mut erigon_tx_cur = erigon_tx.cursor(&tables::BlockTransaction.erased()).await?;
        let mut tx_cur = tx
            .mutable_cursor(&tables::BlockTransaction.erased())
            .await?;

        let mut highest_block = input.stage_progress.unwrap_or(BlockNumber(0));
        let mut walker = erigon_body_cur.walk(Some(
            ErasedTable::<tables::BlockBody>::encode_seek_key(highest_block + 1).to_vec(),
        ));
        let mut batch = Vec::with_capacity(BUFFERING_FACTOR);
        let mut converted = Vec::with_capacity(BUFFERING_FACTOR);

        let mut extracted_blocks_num = 0;
        let mut extracted_txs_num = 0;

        let started_at = Instant::now();
        let done = loop {
            let mut accum_txs = 0;
            while let Some((k, v)) = walker.try_next().await? {
                let body = rlp::decode::<BodyForStorage>(&v)?;
                let base_tx_id = body.base_tx_id;
                let block_tx_base_key = base_tx_id.encode().to_vec();

                let txs = erigon_tx_cur
                    .walk(Some(block_tx_base_key))
                    .take(body.tx_amount)
                    .collect::<anyhow::Result<Vec<_>>>()
                    .await?;

                accum_txs += body.tx_amount;
                batch.push((k, (body, txs)));

                if accum_txs > 500_000 {
                    break;
                }
            }

            if batch.is_empty() {
                break true;
            }

            extracted_blocks_num += batch.len();
            extracted_txs_num += accum_txs;

            batch
                .par_drain(..)
                .map(move |(k, (body, txs))| {
                    let (block_number, block_hash) =
                        ErasedTable::<tables::BlockBody>::decode_key(&k)?;
                    let key = (block_number, block_hash).encode();
                    let value = body.encode().to_vec();
                    Ok::<_, anyhow::Error>((
                        block_number,
                        key,
                        (
                            value,
                            txs.into_iter()
                                .map(|(k, v)| {
                                    Ok((
                                        k,
                                        rlp::decode::<akula::models::Transaction>(&v)?
                                            .encode()
                                            .to_vec(),
                                    ))
                                })
                                .collect::<anyhow::Result<Vec<_>>>()?,
                        ),
                    ))
                })
                .collect_into_vec(&mut converted);

            for res in converted.drain(..) {
                let (block_num, key, (value, txs)) = res?;
                highest_block = block_num;

                body_cur.append((key.into(), value)).await?;

                for (index, tx) in txs {
                    tx_cur.append((index, tx)).await?;
                }
            }

            let now = Instant::now();
            let elapsed = now - started_at;
            if elapsed > Duration::from_secs(30) {
                info!(
                    "Highest block {}, batch size: {} blocks with {} transactions, {} tx/sec",
                    highest_block,
                    extracted_blocks_num,
                    extracted_txs_num,
                    extracted_txs_num as f64
                        / (elapsed.as_secs() as f64 + (elapsed.subsec_millis() as f64 / 1000_f64))
                );

                break false;
            }
        };

        Ok(ExecOutput::Progress {
            stage_progress: highest_block,
            done,
            must_commit: true,
        })
    }
    async fn unwind<'tx>(&self, _: &'tx mut RwTx, _: UnwindInput) -> anyhow::Result<()>
    where
        'db: 'tx,
    {
        todo!()
    }
}

#[derive(Debug)]
struct TerminatingStage;

#[async_trait]
impl<'db, RwTx> Stage<'db, RwTx> for TerminatingStage
where
    RwTx: MutableTransaction<'db>,
{
    fn id(&self) -> StageId {
        StageId("TerminatingStage")
    }
    fn description(&self) -> &'static str {
        "Sync complete, exiting."
    }
    async fn execute<'tx>(&self, _: &'tx mut RwTx, _: StageInput) -> anyhow::Result<ExecOutput>
    where
        'db: 'tx,
    {
        std::process::exit(0)
    }
    async fn unwind<'tx>(&self, _: &'tx mut RwTx, _: UnwindInput) -> anyhow::Result<()>
    where
        'db: 'tx,
    {
        Ok(())
    }
}

async fn execute_with_erigon(erigon_chaindata: PathBuf, chaindata: PathBuf) -> anyhow::Result<()> {
    let erigon_db = Arc::new(akula::MdbxEnvironment::<mdbx::NoWriteMap>::open_ro(
        mdbx::Environment::new(),
        &erigon_chaindata,
        akula::kv::tables::CHAINDATA_TABLES.clone(),
    )?);

    let db = akula::kv::new_database(&chaindata)?;
    {
        let txn = db.begin_mutable().await?;
        if akula::genesis::initialize_genesis(&txn, akula::res::genesis::MAINNET.clone()).await? {
            txn.commit().await?;
        }
    }

    let mut staged_sync = stagedsync::StagedSync::new();
    staged_sync.push(ConvertHeaders {
        db: erigon_db.clone(),
    });
    staged_sync.push(ConvertHeadersTD {
        db: erigon_db.clone(),
    });
    staged_sync.push(ConvertCanonical {
        db: erigon_db.clone(),
    });
    staged_sync.push(ConvertBodies {
        db: erigon_db.clone(),
    });
    staged_sync.push(BlockHashes);
    staged_sync.push(SenderRecovery);
    staged_sync.push(Execution {
        batch_size: 10_000_000_000_u128,
        prune_from: BlockNumber(0),
    });
    staged_sync.push(TerminatingStage);
    staged_sync.run(&db).await?;
}

async fn blockhashes(chaindata: PathBuf) -> anyhow::Result<()> {
    let env = akula::MdbxEnvironment::<mdbx::NoWriteMap>::open_rw(
        mdbx::Environment::new(),
        &chaindata,
        akula::kv::tables::CHAINDATA_TABLES.clone(),
    )?;

    let mut staged_sync = stagedsync::StagedSync::new();
    staged_sync.push(BlockHashes);
    staged_sync.run(&env).await?;
}

async fn table_sizes(chaindata: PathBuf, csv: bool) -> anyhow::Result<()> {
    let env = akula::MdbxEnvironment::<mdbx::NoWriteMap>::open_ro(
        mdbx::Environment::new(),
        &chaindata,
        Default::default(),
    )?;
    let mut sizes = env
        .begin()
        .await?
        .table_sizes()?
        .into_iter()
        .collect::<Vec<_>>();
    sizes.sort_by_key(|(_, size)| *size);

    let mut out = Vec::new();
    if csv {
        out.push("Table,Size".to_string());
        for (table, size) in &sizes {
            out.push(format!("{},{}", table, size));
        }
    } else {
        for (table, size) in &sizes {
            out.push(format!("{} - {}", table, bytesize::ByteSize::b(*size)));
        }
        out.push(format!(
            "TOTAL: {}",
            bytesize::ByteSize::b(sizes.into_iter().map(|(_, size)| size).sum())
        ));
    }

    for line in out {
        println!("{}", line);
    }
    Ok(())
}

async fn db_query(chaindata: PathBuf, table: String, key: Bytes) -> anyhow::Result<()> {
    let env = akula::MdbxEnvironment::<mdbx::NoWriteMap>::open_ro(
        mdbx::Environment::new(),
        &chaindata,
        Default::default(),
    )?;

    let txn = env.begin_ro_txn()?;
    let db = txn
        .open_db(Some(&table))
        .with_context(|| format!("failed to open table: {}", table))?;
    let value = txn.get::<Vec<u8>>(&db, &key)?;

    println!("{:?}", value.as_ref().map(hex::encode));

    if let Some(v) = value {
        println!(
            "{:?}",
            rlp::decode::<akula::models::Transaction>(&v)?.hash()
        );
    }

    Ok(())
}

async fn db_walk(
    chaindata: PathBuf,
    table: String,
    starting_key: Option<Bytes>,
    max_entries: Option<usize>,
) -> anyhow::Result<()> {
    let env = akula::MdbxEnvironment::<mdbx::NoWriteMap>::open_ro(
        mdbx::Environment::new(),
        &chaindata,
        Default::default(),
    )?;

    let txn = env.begin_ro_txn()?;
    let db = txn
        .open_db(Some(&table))
        .with_context(|| format!("failed to open table: {}", table))?;
    let mut cur = txn.cursor(&db)?;
    for (i, item) in if let Some(starting_key) = starting_key {
        cur.iter_from::<Cow<[u8]>, Cow<[u8]>>(&starting_key)
    } else {
        cur.iter::<Cow<[u8]>, Cow<[u8]>>()
    }
    .enumerate()
    .take(max_entries.unwrap_or(usize::MAX))
    {
        let (k, v) = item?;
        println!("{} / {:?} / {:?}", i, hex::encode(k), hex::encode(v));
    }

    Ok(())
}

async fn check_table_eq(db1_path: PathBuf, db2_path: PathBuf, table: String) -> anyhow::Result<()> {
    let env1 = akula::MdbxEnvironment::<mdbx::NoWriteMap>::open_ro(
        mdbx::Environment::new(),
        &db1_path,
        Default::default(),
    )?;
    let env2 = akula::MdbxEnvironment::<mdbx::NoWriteMap>::open_ro(
        mdbx::Environment::new(),
        &db2_path,
        Default::default(),
    )?;

    let txn1 = env1.begin_ro_txn()?;
    let txn2 = env2.begin_ro_txn()?;
    let db1 = txn1
        .open_db(Some(&table))
        .with_context(|| format!("failed to open table: {}", table))?;
    let db2 = txn2
        .open_db(Some(&table))
        .with_context(|| format!("failed to open table: {}", table))?;
    let mut cur1 = txn1.cursor(&db1)?;
    let mut cur2 = txn2.cursor(&db2)?;

    let mut excess = 0;
    for (i, res) in cur1
        .iter_start::<Cow<[u8]>, Cow<[u8]>>()
        .zip_longest(cur2.iter_start::<Cow<[u8]>, Cow<[u8]>>())
        .enumerate()
    {
        if i % 1_000_000 == 0 {
            info!("Checked {} entries", i);
        }
        match res {
            itertools::EitherOrBoth::Both(a, b) => {
                let (k1, v1) = a?;
                let (k2, v2) = b?;
                ensure!(
                    k1 == k2 && v1 == v2,
                    "MISMATCH DETECTED: {}: {} != {}: {}",
                    hex::encode(k1),
                    hex::encode(v1),
                    hex::encode(k2),
                    hex::encode(v2)
                );
            }
            itertools::EitherOrBoth::Left(_) => excess -= 1,
            itertools::EitherOrBoth::Right(_) => excess += 1,
        }
    }

    match excess.cmp(&0) {
        std::cmp::Ordering::Less => {
            bail!("db1 longer than db2 by {} entries", -excess);
        }
        std::cmp::Ordering::Equal => {}
        std::cmp::Ordering::Greater => {
            bail!("db2 longer than db1 by {} entries", excess);
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();

    let filter = if std::env::var(EnvFilter::DEFAULT_ENV)
        .unwrap_or_default()
        .is_empty()
    {
        EnvFilter::new("akula=info")
    } else {
        EnvFilter::from_default_env()
    };
    tracing_subscriber::registry()
        // the `TasksLayer` can be used in combination with other `tracing` layers...
        .with(tracing_subscriber::fmt::layer().with_target(false))
        .with(filter)
        .init();

    match opt {
        Opt::DbStats { chaindata, csv } => table_sizes(chaindata, csv).await?,
        Opt::Blockhashes { chaindata } => blockhashes(chaindata).await?,
        Opt::DbQuery {
            chaindata,
            table,
            key,
        } => db_query(chaindata, table, key).await?,
        Opt::DbWalk {
            chaindata,
            table,
            starting_key,
            max_entries,
        } => db_walk(chaindata, table, starting_key, max_entries).await?,
        Opt::CheckEqual { db1, db2, table } => check_table_eq(db1, db2, table).await?,
        Opt::ExecuteWithErigon {
            chaindata,
            erigon_chaindata,
        } => execute_with_erigon(erigon_chaindata, chaindata).await?,
    }

    Ok(())
}
