// Copyright (C) 2020  Braiins Systems s.r.o.
//
// This file is part of Braiins Open-Source Initiative (BOSI).
//
// BOSI is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.
//
// Please, keep in mind that we may also license BOSI or any part thereof
// under a proprietary license. For more information on the terms and conditions
// of such proprietary license or if you have any other questions, please
// contact us at opensource@braiins.com.

use crate::error;
use crate::job;
use crate::node;
use crate::stats;
use crate::sync;
use crate::work;

use bosminer_macros::ClientNode;

use ii_bitcoin::{FromHex, HashTrait as _};
use ii_stats::WindowedTimeMean;

use async_trait::async_trait;
use futures::channel::mpsc;
use futures::lock::Mutex;
use ii_async_compat::prelude::*;
use ii_async_compat::select;
use tokio::time::delay_for;

use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time;

#[derive(Debug)]
pub struct Job {
    client: Weak<Client>,
    difficulty: Difficulty,
    prev_hash: ii_bitcoin::DHash,
    merkle_root: ii_bitcoin::DHash,
}

impl Job {
    fn new(client: Arc<Client>, difficulty: Difficulty, index: u64) -> Self {
        let mut merkle_root_bytes = [0u8; ii_bitcoin::SHA256_DIGEST_SIZE];
        merkle_root_bytes[..std::mem::size_of::<u64>()].copy_from_slice(&u64::to_le_bytes(index));
        let merkle_root = ii_bitcoin::DHash::from_slice(&merkle_root_bytes)
            .expect("BUG: cannot convert double hash from slice");

        Self {
            client: Arc::downgrade(&client),
            difficulty,
            prev_hash: ii_bitcoin::DHash::from_hex(
                "0000000000000000000ce42cebccbafe38380349f00115366d339e9e20a832f4",
            )
            .expect("BUG: parse hex"),
            merkle_root,
        }
    }
}

impl job::Bitcoin for Job {
    fn origin(&self) -> Weak<dyn node::Client> {
        self.client.clone()
    }

    fn version(&self) -> u32 {
        536928256
    }

    fn version_mask(&self) -> u32 {
        0x1fffe000
    }

    fn previous_hash(&self) -> &ii_bitcoin::DHash {
        &self.prev_hash
    }

    fn merkle_root(&self) -> &ii_bitcoin::DHash {
        &self.merkle_root
    }

    fn time(&self) -> u32 {
        1581508326
    }

    fn bits(&self) -> u32 {
        387062484
    }

    fn target(&self) -> ii_bitcoin::Target {
        self.difficulty.to_target()
    }

    fn is_valid(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone)]
struct Difficulty {
    index: Arc<AtomicUsize>,
}

impl Difficulty {
    const INITIAL_DIFFICULTY: usize = 512;
    const MINIMAL_DIFFICULTY: usize = 1;

    const DIFFICULTY_STEP: usize = 128;
    const INITIAL_DIFFICULTY_INDEX: usize = Self::INITIAL_DIFFICULTY / Self::DIFFICULTY_STEP;

    #[inline]
    fn get_index(&self) -> usize {
        self.index.load(Ordering::Relaxed)
    }

    #[inline]
    fn inc(&self) {
        self.index.fetch_add(1, Ordering::Relaxed);
    }

    fn dec(&self, half: bool) {
        let mut index = self.get_index();
        loop {
            let new_index = match index {
                0 => break,
                1 => 0,
                _ if half => index / 2,
                _ => index - 1,
            };
            let old_index = index;

            index = self
                .index
                .compare_and_swap(index, new_index, Ordering::Relaxed);
            if index == old_index {
                break;
            }
        }
    }

    fn to_target(&self) -> ii_bitcoin::Target {
        let index = self.get_index();
        let difficulty = if index > 0 {
            Self::DIFFICULTY_STEP * index
        } else {
            Self::MINIMAL_DIFFICULTY
        };
        ii_bitcoin::Target::from_pool_difficulty(difficulty)
    }
}

impl Default for Difficulty {
    fn default() -> Self {
        Self {
            index: Arc::new(AtomicUsize::new(Self::INITIAL_DIFFICULTY_INDEX)),
        }
    }
}

// TODO: Use PID regulator
struct DifficultyRegulator {
    client: Arc<Client>,
    difficulty: Difficulty,
    last_accepted: stats::Snapshot<stats::MeterSnapshot>,
    solutions_per_sec_avg: WindowedTimeMean,
}

impl DifficultyRegulator {
    const SOLUTIONS_INTERVAL: time::Duration = time::Duration::from_secs(120);

    async fn new(client: Arc<Client>, difficulty: Difficulty) -> Self {
        Self {
            difficulty,
            last_accepted: client.stats.accepted.take_snapshot().await,
            solutions_per_sec_avg: WindowedTimeMean::new(Self::SOLUTIONS_INTERVAL),
            client,
        }
    }

    async fn recalculate_target(&mut self) {
        if *self.client.stats.generated_work.take_snapshot() <= 0 {
            return;
        }

        let accepted = self.client.stats.accepted.take_snapshot().await;
        let elapsed = accepted
            .snapshot_time
            .checked_duration_since(self.last_accepted.snapshot_time)
            .expect("BUG: accepted snapshot time");
        let solutions_per_sec =
            (accepted.solutions - self.last_accepted.solutions) as f64 / elapsed.as_secs_f64();

        self.solutions_per_sec_avg
            .insert(solutions_per_sec, accepted.snapshot_time);

        if self.solutions_per_sec_avg.measure(accepted.snapshot_time) > 10.0 {
            self.difficulty.inc();
        } else if solutions_per_sec < 3.0 {
            self.difficulty.dec(solutions_per_sec < 0.1);
        }

        self.last_accepted = accepted;
    }
}

#[derive(Debug, ClientNode)]
pub struct Client {
    description: String,
    #[member_status]
    status: sync::StatusMonitor,
    #[member_client_stats]
    stats: stats::BasicClient,
    stop_sender: mpsc::Sender<()>,
    stop_receiver: Mutex<mpsc::Receiver<()>>,
    last_job: Mutex<Option<Arc<Job>>>,
    job_sender: Mutex<job::Sender>,
    solution_receiver: Mutex<job::SolutionReceiver>,
}

impl Client {
    const NEW_JOB_INTERVAL: time::Duration = time::Duration::from_secs(10);

    pub fn new(description: String, solver: job::Solver) -> Self {
        let (stop_sender, stop_receiver) = mpsc::channel(1);
        Self {
            description,
            status: Default::default(),
            stats: Default::default(),
            stop_sender,
            stop_receiver: Mutex::new(stop_receiver),
            last_job: Mutex::new(None),
            job_sender: Mutex::new(solver.job_sender),
            solution_receiver: Mutex::new(solver.solution_receiver),
        }
    }

    async fn update_last_job(&self, job: Arc<Job>) {
        self.last_job.lock().await.replace(job);
    }

    async fn last_job(&self) -> Option<Arc<Job>> {
        self.last_job.lock().await.as_ref().map(|job| job.clone())
    }

    async fn send_job_and_wait(self: Arc<Self>, difficulty: Difficulty, index: &mut u64) {
        let job = Arc::new(Job::new(self.clone(), difficulty, *index));
        *index += 1;

        self.update_last_job(job.clone()).await;
        self.job_sender.lock().await.send(job);

        delay_for(Self::NEW_JOB_INTERVAL).await;
    }

    async fn account_solution(&self, solution: work::Solution) {
        let now = std::time::Instant::now();
        self.stats
            .accepted
            .account_solution(&solution.job_target(), now)
            .await;
    }

    async fn main_loop(self: Arc<Self>) -> error::Result<()> {
        unimplemented!()
        // let mut solution_receiver = self.solution_receiver.lock().await;
        //
        // let difficulty: Difficulty = Default::default();
        // let mut regulator = DifficultyRegulator::new(self.clone(), difficulty.clone()).await;
        // let mut index = 0;
        //
        // while !self.status.is_shutting_down() {
        //     select! {
        //         _ = self.clone().send_job_and_wait(difficulty.clone(), &mut index).fuse() => {}
        //         solution = solution_receiver.receive().fuse() => {
        //             match solution {
        //                 Some(solution) => self.account_solution(solution).await,
        //                 None => {
        //                     // TODO: initiate Destroying and remove error
        //                     Err("Standard application shutdown")?;
        //                 }
        //             }
        //         }
        //     }
        //     regulator.recalculate_target().await;
        // }
        // Ok(())
    }

    async fn run(self: Arc<Self>) {
        if self.status.initiate_running() {
            if let Err(_) = self.clone().main_loop().await {
                self.status.initiate_failing();
            }
        }
    }

    async fn main_task(self: Arc<Self>) {
        unimplemented!()
        // loop {
        //     let mut stop_receiver = self.stop_receiver.lock().await;
        //     select! {
        //         _ = self.clone().run().fuse() => {}
        //         _ = stop_receiver.next() => {}
        //     }
        //
        //     // Invalidate current job to stop working on it
        //     self.job_sender.lock().await.invalidate();
        //
        //     if self.status.can_stop() {
        //         // NOTE: it is not safe to add here any code!
        //         break;
        //     }
        //     // Restarting
        // }
    }
}

#[async_trait]
impl node::Client for Client {
    fn start(self: Arc<Self>) {
        tokio::spawn(self.clone().main_task());
    }

    fn stop(&self) {
        if let Err(e) = self.stop_sender.clone().try_send(()) {
            assert!(
                e.is_full(),
                "BUG: Unexpected error in stop sender: {}",
                e.to_string()
            );
        }
    }

    async fn get_last_job(&self) -> Option<Arc<dyn job::Bitcoin>> {
        self.last_job()
            .await
            .map(|job| job as Arc<dyn job::Bitcoin>)
    }
}

impl fmt::Display for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.description)
    }
}
