pub mod difficulty;

use super::Consensus;
use crate::models::*;
use async_trait::*;
use ethereum_types::*;
use std::{collections::BTreeMap, fmt::Debug};
use thiserror::Error;

#[derive(Debug)]
pub struct Ethash {
    duration_limit: u64,
    block_reward: BTreeMap<BlockNumber, U256>,
    homestead_formula: Option<BlockNumber>,
    byzantium_adj_factor: Option<BlockNumber>,
    difficulty_bomb: Option<DifficultyBomb>,
}

#[derive(Debug, Error)]
pub enum ValidationError {
    #[error("invalid difficulty (expected {expected:?}, got {got:?})")]
    WrongDifficulty { expected: U256, got: U256 },
}

#[async_trait]
impl Consensus for Ethash {
    async fn verify_header(
        &self,
        header: &BlockHeader,
        parent: &BlockHeader,
    ) -> anyhow::Result<()> {
        let block_params = BlockEthashParams {
            duration_limit: self.duration_limit,
            block_reward: {
                let mut reward = U256::zero();

                for (after, block_reward_after) in self.block_reward.iter().rev() {
                    if header.number >= *after {
                        reward = *block_reward_after;
                        break;
                    }
                }

                reward
            },
            homestead_formula: self
                .homestead_formula
                .map(|after| header.number >= after)
                .unwrap_or(false),
            byzantium_adj_factor: self
                .byzantium_adj_factor
                .map(|after| header.number >= after)
                .unwrap_or(false),
            difficulty_bomb: self.difficulty_bomb.map(|difficulty_bomb| {
                let mut delay_to = BlockNumber(0);
                for (&after, &delay_to_entry) in difficulty_bomb.delays.iter().rev() {
                    if header.number >= after {
                        delay_to = delay_to_entry;
                        break;
                    }
                }

                BlockDifficultyBomb { delay_to }
            }),
        };

        // TODO: port Ethash PoW verification
        // let epoch_number = {header.number / ethash::epoch_length};
        // auto epoch_context{ethash::create_epoch_context(static_cast<int>(epoch_number))};

        // auto boundary256{header.boundary()};
        // auto seal_hash(header.hash(/*for_sealing =*/true));
        // ethash::hash256 sealh256{*reinterpret_cast<ethash::hash256*>(seal_hash.bytes)};
        // ethash::hash256 mixh256{};
        // std::memcpy(mixh256.bytes, header.mix_hash.bytes, 32);

        // uint64_t nonce{endian::load_big_u64(header.nonce.data())};
        // return ethash::verify(*epoch_context, sealh256, mixh256, nonce, boundary256) ? ValidationError::Ok
        //                                                                              : ValidationError::InvalidSeal;

        let parent_has_uncles = parent.ommers_hash != EMPTY_LIST_HASH;
        let difficulty = difficulty::canonical_difficulty(
            header.number,
            header.timestamp,
            parent.difficulty,
            parent.timestamp,
            parent_has_uncles,
            &block_params,
        );
        if difficulty != header.difficulty {
            return Err(ValidationError::WrongDifficulty {
                expected: difficulty,
                got: header.difficulty,
            }
            .into());
        }
        Ok(())
    }
}
