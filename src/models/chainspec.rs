use super::BlockNumber;
use crate::util::*;
use bytes::Bytes;
use ethereum_types::*;
use serde::*;
use std::{
    collections::{BTreeMap, HashMap},
    time::Duration,
};

type NodeUrl = String;

#[derive(Debug, Deserialize, PartialEq)]
struct ChainSpec {
    name: String,
    data_dir: String,
    bootnodes: Vec<NodeUrl>,
    engine: Engine,
    upgrades: Upgrades,
    params: Params,
    genesis: Genesis,
    contracts: HashMap<BlockNumber, HashMap<Address, Contract>>,
    balances: HashMap<BlockNumber, HashMap<Address, U256>>,
}

#[derive(Debug, Deserialize, PartialEq)]
struct DifficultyBomb {
    delays: BTreeMap<BlockNumber, BlockNumber>,
}

#[derive(Debug, Deserialize, PartialEq)]
enum Engine {
    Clique {
        #[serde(deserialize_with = "deserialize_period_as_duration")]
        period: Duration,
        epoch: u64,
        genesis: CliqueGenesis,
    },
    Ethash {
        duration_limit: u64,
        block_reward: BTreeMap<BlockNumber, U256>,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "::serde_with::rust::unwrap_or_skip"
        )]
        homestead_formula: Option<BlockNumber>,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "::serde_with::rust::unwrap_or_skip"
        )]
        byzantium_adj_factor: Option<BlockNumber>,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "::serde_with::rust::unwrap_or_skip"
        )]
        difficulty_bomb: Option<DifficultyBomb>,
        genesis: EthashGenesis,
    },
}

#[derive(Debug, Deserialize, PartialEq)]
struct CliqueGenesis {
    vanity: H256,
    signers: Vec<Address>,
}

#[derive(Debug, Deserialize, PartialEq)]
struct EthashGenesis {
    nonce: H64,
    mix_hash: H256,
}

#[derive(Debug, Deserialize, PartialEq)]
struct EnableDisable {
    enable: BlockNumber,
    disable: BlockNumber,
}

// deserialize_str_as_u64
#[derive(Debug, Deserialize, PartialEq)]
struct Upgrades {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "::serde_with::rust::unwrap_or_skip"
    )]
    homestead: Option<BlockNumber>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "::serde_with::rust::unwrap_or_skip"
    )]
    tangerine: Option<BlockNumber>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "::serde_with::rust::unwrap_or_skip"
    )]
    spurious: Option<BlockNumber>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "::serde_with::rust::unwrap_or_skip"
    )]
    byzantium: Option<BlockNumber>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "::serde_with::rust::unwrap_or_skip"
    )]
    constantinople: Option<BlockNumber>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "::serde_with::rust::unwrap_or_skip"
    )]
    petersburg: Option<BlockNumber>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "::serde_with::rust::unwrap_or_skip"
    )]
    istanbul: Option<BlockNumber>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "::serde_with::rust::unwrap_or_skip"
    )]
    berlin: Option<BlockNumber>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "::serde_with::rust::unwrap_or_skip"
    )]
    london: Option<BlockNumber>,
}

#[derive(Debug, Deserialize, PartialEq)]
struct Params {
    chain_id: u64,
    maximum_extra_data_size: u64,
    min_gas_limit: u64,
    network_id: u64,
}

#[derive(Debug, Deserialize, PartialEq)]
struct Genesis {
    gas_limit: u64,
    timestamp: u64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
enum Contract {
    Contract {
        #[serde(deserialize_with = "deserialize_str_as_bytes")]
        code: Bytes,
    },
    Precompile(Precompile),
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
enum ModExpVersion {
    ModExp198,
    ModExp2565,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
enum Precompile {
    EcRecover { base: u64, word: u64 },
    Sha256 { base: u64, word: u64 },
    Ripemd160 { base: u64, word: u64 },
    Identity { base: u64, word: u64 },
    ModExp { version: ModExpVersion },
    AltBn128Add { price: u64 },
    AltBn128Mul { price: u64 },
    AltBn128Pairing { base: u64, pair: u64 },
    Blake2F { gas_per_round: u64 },
}

struct DeserializePeriodAsDuration;

impl<'de> de::Visitor<'de> for DeserializePeriodAsDuration {
    type Value = Duration;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("an u64")
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Duration::from_millis(v))
    }
}

fn deserialize_period_as_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: de::Deserializer<'de>,
{
    deserializer.deserialize_any(DeserializePeriodAsDuration)
}

fn deserialize_str_as_u64<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: de::Deserializer<'de>,
{
    U64::deserialize(deserializer).map(|num| num.as_u64())
}
#[cfg(test)]
mod tests {
    use super::*;
    use hex_literal::hex;
    use maplit::hashmap;

    #[test]
    fn load_chainspec() {
        let chain_spec = ron::from_str::<ChainSpec>(include_str!("chains/rinkeby.ron")).unwrap();

        assert_eq!(
            ChainSpec {
                name: "Rinkeby".into(),
                data_dir: "rinkeby".into(),
                bootnodes: vec![
                    "enode://a24ac7c5484ef4ed0c5eb2d36620ba4e4aa13b8c84684e1b4aab0cebea2ae45cb4d375b77eab56516d34bfbd3c1a833fc51296ff084b770b94fb9028c4d25ccf@52.169.42.101:30303".into(),
                    "enode://343149e4feefa15d882d9fe4ac7d88f885bd05ebb735e547f12e12080a9fa07c8014ca6fd7f373123488102fe5e34111f8509cf0b7de3f5b44339c9f25e87cb8@52.3.158.184:30303".into(),
                    "enode://b6b28890b006743680c52e64e0d16db57f28124885595fa03a562be1d2bf0f3a1da297d56b13da25fb992888fd556d4c1a27b1f39d531bde7de1921c90061cc6@159.89.28.211:30303".into(),
                ],
                engine: Engine::Clique {
                    period: Duration::from_millis(15),
                    epoch: 30_000,
                    genesis: CliqueGenesis {
                        vanity: hex!("52657370656374206d7920617574686f7269746168207e452e436172746d616e").into(), 
                        signers: vec![
                            hex!("42eb768f2244c8811c63729a21a3569731535f06").into(),
                            hex!("7ffc57839b00206d1ad20c69a1981b489f772031").into(),
                            hex!("b279182d99e65703f0076e4812653aab85fca0f0").into(),
                        ],
                    }
                },
                upgrades: Upgrades {
                    homestead: Some(1150000.into()),
                    tangerine: Some(2463000.into()),
                    spurious: Some(2675000.into()),
                    byzantium: Some(4370000.into()),
                    constantinople: Some(7280000.into()),
                    petersburg: Some(7280000.into()),
                    istanbul: Some(9069000.into()),
                    berlin: Some(12244000.into()),
                    london: Some(12965000.into()),
                },
                params: Params {
                    chain_id: 4,
                    network_id: 4,
                    maximum_extra_data_size: 65535,
                    min_gas_limit: 5000,
                },
                genesis: Genesis {
                    gas_limit: 0x47b760,
                    timestamp: 0x58ee40ba,
                },
                contracts: hashmap! {
                    0.into() => hashmap! {
                        hex!("0000000000000000000000000000000000000001").into() => Contract::Precompile(Precompile::EcRecover {
                            base: 3000,
                            word: 0,
                        }),
                        hex!("0000000000000000000000000000000000000002").into() => Contract::Precompile(Precompile::Sha256 {
                            base: 60,
                            word: 12,
                        }),
                        hex!("0000000000000000000000000000000000000003").into() => Contract::Precompile(Precompile::Ripemd160 {
                            base: 600,
                            word: 120,
                        }),
                        hex!("0000000000000000000000000000000000000004").into() => Contract::Precompile(Precompile::Identity {
                            base: 15,
                            word: 3,
                        }),
                    },
                    1035301.into() => hashmap! {
                        hex!("0000000000000000000000000000000000000005").into() => Contract::Precompile(Precompile::ModExp {
                            version: ModExpVersion::ModExp198,
                        }),
                        hex!("0000000000000000000000000000000000000006").into() => Contract::Precompile(Precompile::AltBn128Add {
                            price: 500,
                        }),
                        hex!("0000000000000000000000000000000000000007").into() => Contract::Precompile(Precompile::AltBn128Mul {
                            price: 40000,
                        }),
                        hex!("0000000000000000000000000000000000000008").into() => Contract::Precompile(Precompile::AltBn128Pairing {
                            base: 100000,
                            pair: 80000,
                        }),
                    },
                    5435345.into() => hashmap! {
                        hex!("0000000000000000000000000000000000000006").into() => Contract::Precompile(Precompile::AltBn128Add {
                            price: 150,
                        }),
                        hex!("0000000000000000000000000000000000000007").into() => Contract::Precompile(Precompile::AltBn128Mul {
                            price: 6000,
                        }),
                        hex!("0000000000000000000000000000000000000008").into() => Contract::Precompile(Precompile::AltBn128Pairing {
                            base: 45000,
                            pair: 34000,
                        }),
                        hex!("0000000000000000000000000000000000000009").into() => Contract::Precompile(Precompile::Blake2F {
                            gas_per_round: 1,
                        }),
                    },
                    8290928.into() => hashmap! {
                        hex!("0000000000000000000000000000000000000005").into() => Contract::Precompile(Precompile::ModExp {
                            version: ModExpVersion::ModExp2565,
                        })
                    }
                },
                balances: hashmap! {
                    0.into() => hashmap! {
                        hex!("31b98d14007bdee637298086988a0bbd31184523").into() => "0x200000000000000000000000000000000000000000000000000000000000000".into(),
                    },
                },
            },
            chain_spec,
        );
    }
}
