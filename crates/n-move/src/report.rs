// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! n-move's records of a simulated run, in the layout pinned in MystenLabs/n-version-move#17.

use std::{
    fs::File,
    io::{BufWriter, Write},
    path::Path,
    time::Duration,
};

use execution::engine::ExecutionEngine;
use eyre::{Context, Result};
use serde::Serialize;
use serde_json::{Map, Value, json};

use crate::{
    assignment::VersionId,
    decision::Rejection,
    engine::NMoveConfig,
    processor::{NVersionProcessor, Outcome, Records, TransactionRecord},
};

/// Every validator's records of a run, by authority.
pub struct RunRecords {
    versions: Vec<VersionId>,
    records: Vec<Records>,
    /// Simulated time the load started at.
    start_ms: u64,
}

impl RunRecords {
    pub fn new<E: ExecutionEngine>(processors: Vec<NVersionProcessor<E>>, start: Duration) -> Self {
        let (versions, records) = processors
            .into_iter()
            .map(|processor| (processor.version(), processor.into_parts().2))
            .unzip();
        Self {
            versions,
            records,
            start_ms: start.as_millis() as u64,
        }
    }

    /// `transactions.jsonl`: every validator's record of every transaction.
    pub fn write_transactions(&self, dir: &Path) -> Result<()> {
        let mut lines = Self::create(&dir.join("transactions.jsonl"))?;
        for (authority, records) in self.records.iter().enumerate() {
            for record in &records.transactions {
                let (outcome, reason) = record.outcome.names();
                let digest: String = record
                    .digest
                    .as_bytes()
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect();
                let line = json!({
                    "authority": authority,
                    "version": self.versions[authority].0,
                    "commit": record.position.commit,
                    "index": record.position.index,
                    "digest": digest,
                    "submitted_ms": record.submitted_ms,
                    "committed_ms": record.committed_ms,
                    "executed_ms": record.executed_ms,
                    "voted_ms": record.voted_ms,
                    "decided_ms": record.decided_ms,
                    "outcome": outcome,
                    "reason": reason,
                    "lost": record.lost,
                    "lost_to_earlier": record.lost_to_earlier,
                    "repairs": record.repairs,
                    "gas_used": record.gas_used,
                });
                writeln!(lines, "{line}")?;
            }
        }
        Ok(lines.flush()?)
    }

    /// `blocks.jsonl`: the committed blocks' vote bytes, the same at every validator.
    pub fn write_blocks(&self, dir: &Path) -> Result<()> {
        let mut lines = Self::create(&dir.join("blocks.jsonl"))?;
        for block in &self.records[0].blocks {
            let line = json!({
                "round": block.round,
                "author": block.author.index(),
                "vote_bytes": block.vote_bytes,
            });
            writeln!(lines, "{line}")?;
        }
        Ok(lines.flush()?)
    }

    /// The summary fields n-move adds to a run's: its parameters, decisions, and their timing.
    pub fn summary(&self, config: &NMoveConfig) -> Map<String, Value> {
        // Decisions agree across validators, so count one validator's.
        let decided = &self.records[0].transactions;
        let named = |name| {
            let names = decided.iter().map(|record| record.outcome.names());
            names
                .filter(|(outcome, reason)| *outcome == name || *reason == Some(name))
                .count()
        };
        let all = || {
            self.records
                .iter()
                .flat_map(|records| &records.transactions)
        };
        let latency = |stage: fn(&TransactionRecord) -> Option<u64>| {
            let latencies = all().filter_map(|record| {
                stage(record).map(|time| time.saturating_sub(record.submitted_ms))
            });
            Percentiles::new(latencies.collect())
        };
        let last_ms = all().map(|record| record.decided_ms).max();
        let duration_ms = last_ms.unwrap_or_default().saturating_sub(self.start_ms);
        let throughput_tps = match duration_ms {
            0 => 0.0,
            duration_ms => decided.len() as f64 * 1000.0 / duration_ms as f64,
        };
        let vote_bytes: Vec<_> = self.records[0]
            .blocks
            .iter()
            .map(|block| block.vote_bytes)
            .collect();
        let summary = json!({
            "parameters": {
                "faults": config.faults,
                "wrong": config.wrong,
                "threshold": config.threshold(),
                "cutoff_rounds": config.cutoff_rounds,
                "versions": config.versions.unwrap_or(self.records.len()),
            },
            "duration_ms": duration_ms,
            "transactions": decided.len(),
            "throughput_tps": throughput_tps,
            "outcomes": {
                "final_correct": named("final_correct"),
                "final_wrong": named("final_wrong"),
                "rejected": named("rejected"),
            },
            "rejections": {
                "impossible": named("impossible"),
                "cutoff": named("cutoff"),
                "earlier_transaction": named("earlier_transaction"),
            },
            "losses": {
                "lost": all().filter(|record| record.lost).count(),
                "lost_to_earlier": all().filter(|record| record.lost_to_earlier).count(),
            },
            "latency_ms": {
                "committed": latency(|record| Some(record.committed_ms)),
                "executed": latency(|record| Some(record.executed_ms)),
                "voted": latency(|record| record.voted_ms),
                "decided": latency(|record| Some(record.decided_ms)),
            },
            "repairs": all().map(|record| u64::from(record.repairs)).sum::<u64>(),
            "vote_bytes_per_block": {
                "mean": vote_bytes.iter().sum::<usize>() as f64 / vote_bytes.len().max(1) as f64,
                "max": vote_bytes.iter().max().copied().unwrap_or(0),
            },
        });
        let Value::Object(summary) = summary else {
            unreachable!("a summary is a JSON object");
        };
        summary
    }

    fn create(path: &Path) -> Result<BufWriter<File>> {
        let file =
            File::create(path).wrap_err_with(|| format!("Failed to create {}", path.display()))?;
        Ok(BufWriter::new(file))
    }
}

impl Outcome {
    /// The outcome and, for rejections, the reason, as the outputs name them.
    fn names(&self) -> (&'static str, Option<&'static str>) {
        match self {
            Self::FinalCorrect => ("final_correct", None),
            Self::FinalWrong => ("final_wrong", None),
            Self::Rejected(Rejection::Impossible) => ("rejected", Some("impossible")),
            Self::Rejected(Rejection::Cutoff) => ("rejected", Some("cutoff")),
            Self::Rejected(Rejection::EarlierTransaction) => {
                ("rejected", Some("earlier_transaction"))
            }
        }
    }
}

/// By nearest rank; none without samples.
#[derive(Serialize, PartialEq, Debug)]
struct Percentiles {
    p50: Option<u64>,
    p90: Option<u64>,
    p99: Option<u64>,
}

impl Percentiles {
    fn new(mut samples: Vec<u64>) -> Self {
        samples.sort_unstable();
        let rank = |p: f64| {
            let rank = (p * samples.len() as f64).ceil() as usize;
            samples.get(rank.max(1) - 1).copied()
        };
        Self {
            p50: rank(0.5),
            p90: rank(0.9),
            p99: rank(0.99),
        }
    }
}
