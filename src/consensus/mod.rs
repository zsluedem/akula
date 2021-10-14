pub mod ethash;

use crate::models::*;
use anyhow::bail;
use async_trait::*;
use ethereum_types::*;
use std::{collections::BTreeMap, fmt::Debug};
use thiserror::Error;

#[async_trait]
pub trait Consensus: Debug + Send + Sync {
    async fn verify_header(&self, header: &BlockHeader, parent: &BlockHeader)
        -> anyhow::Result<()>;
}

#[derive(Debug)]
pub struct NoProof;

#[async_trait]
impl Consensus for NoProof {
    async fn verify_header(&self, _: &BlockHeader, parent: &BlockHeader) -> anyhow::Result<()> {
        Ok(())
    }
}

pub type Clique = NoProof;
pub type AuRa = NoProof;

pub fn init_consensus(params: ConsensusParams) -> anyhow::Result<Box<dyn Consensus>> {
    Ok(match params {
        ConsensusParams::Clique { period, epoch } => bail!("Clique is not yet implemented"),
        ConsensusParams::Ethash {
            duration_limit,
            block_reward,
            homestead_formula,
            byzantium_adj_factor,
            difficulty_bomb,
        } => Box::new(ethash::Ethash {
            duration_limit,
            block_reward,
            homestead_formula,
            byzantium_adj_factor,
            difficulty_bomb,
        }),
        ConsensusParams::NoProof => Box::new(NoProof),
    })
}
