// Copyright (C) 2019  Braiins Systems s.r.o.
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

//! NOTE: hacked copy of V2 with channel only support

use ii_logging::macros::*;

use crate::error;
use crate::job;
use crate::node;
use crate::stats;
use crate::sync;
use crate::work;

use failure::ResultExt;

use ii_bitcoin::HashTrait;

use bosminer_config::{ClientDescriptor, ClientProtocol};
use bosminer_macros::ClientNode;

use async_trait::async_trait;
use futures::channel::mpsc;
use futures::lock::Mutex;
use ii_async_compat::prelude::*;
use ii_async_compat::select;

use std::collections::VecDeque;
use std::fmt;
use std::net::ToSocketAddrs;
use std::sync::{Arc, Weak};
use std::time;

use ii_stratum::v2::framing::{Framing, Header};
use ii_stratum::v2::messages::{
    NewMiningJob, OpenStandardMiningChannel, OpenStandardMiningChannelError,
    OpenStandardMiningChannelSuccess, SetNewPrevHash, SetTarget, SetupConnection,
    SetupConnectionError, SetupConnectionSuccess, SubmitSharesError, SubmitSharesStandard,
    SubmitSharesSuccess,
};
use ii_stratum::v2::types::DeviceInfo;
use ii_stratum::v2::types::*;
use ii_stratum::v2::{build_message_from_frame, Handler};
use ii_stratum::{v1, v2};
use ii_stratum_proxy::translation::{V2ToV1Translation, V2ToV1TranslationOptions};
use ii_wire::Connection;

use std::collections::HashMap;

// TODO: move it to the stratum crate
const VERSION_MASK: u32 = 0x1fffe000;

#[derive(Debug)]
pub struct ConnectionDetails {
    pub user: String,
    pub host: String,
    pub port: u16,
    pub fragment: Option<String>,
}

impl ConnectionDetails {
    pub fn from_descriptor(descriptor: &ClientDescriptor) -> Self {
        Self {
            user: descriptor.user.clone(),
            host: descriptor.host.clone(),
            port: descriptor.port(),
            fragment: descriptor.fragment.clone(),
        }
    }

    fn get_host_and_port(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    fn try_enable_xnsub(&self) -> bool {
        self.host.find(".nicehash.com").is_some()
            || self
                .fragment
                .as_ref()
                .and_then(|fragment| fragment.find("xnsub"))
                .is_some()
    }
}

#[derive(Debug, Clone)]
pub struct StratumJob {
    client: Weak<StratumClient>,
    id: u32,
    channel_id: u32,
    version: u32,
    prev_hash: ii_bitcoin::DHash,
    merkle_root: ii_bitcoin::DHash,
    time: u32,
    bits: u32,
    target: ii_bitcoin::Target,
}

impl StratumJob {
    pub fn new(
        client: Arc<StratumClient>,
        job_msg: &NewMiningJob,
        prevhash_msg: &SetNewPrevHash,
        target: ii_bitcoin::Target,
    ) -> Self {
        Self {
            client: Arc::downgrade(&client),
            id: job_msg.job_id,
            channel_id: job_msg.channel_id,
            version: job_msg.version,
            prev_hash: ii_bitcoin::DHash::from_slice(prevhash_msg.prev_hash.as_ref())
                .expect("BUG: Stratum: incorrect size of prev hash"),
            merkle_root: ii_bitcoin::DHash::from_slice(job_msg.merkle_root.as_ref())
                .expect("BUG: Stratum: incorrect size of merkle root"),
            time: prevhash_msg.min_ntime,
            bits: prevhash_msg.nbits,
            target,
        }
    }
}

impl job::Bitcoin for StratumJob {
    fn origin(&self) -> Weak<dyn node::Client> {
        self.client.clone()
    }

    fn version(&self) -> u32 {
        self.version
    }

    fn version_mask(&self) -> u32 {
        VERSION_MASK
    }

    fn previous_hash(&self) -> &ii_bitcoin::DHash {
        &self.prev_hash
    }

    fn merkle_root(&self) -> &ii_bitcoin::DHash {
        &self.merkle_root
    }

    fn time(&self) -> u32 {
        self.time
    }

    fn bits(&self) -> u32 {
        self.bits
    }

    fn target(&self) -> ii_bitcoin::Target {
        self.target
    }

    fn is_valid(&self) -> bool {
        // TODO: currently there is no easy way to detect the job is valid -> we have to check
        //  its presence in the registry. The inequality below was possible in the previous
        //  iteration of the protocol
        // self.block_height >= self.current_block_height.load(Ordering::Relaxed)
        true
    }
}

/// Queue that contains pairs of solution and its assigned sequence number. It is our responsibility
/// to keep the sequence number monotonic so that we as a stratum V2 client can easily process bulk
/// acknowledgements. The sequence number type has been selected as u32 to match
/// up with the protocol.
type SolutionQueue = Mutex<VecDeque<(work::Solution, u32)>>;

/// Helper task for `StratumClient` that implements Stratum V2 visitor which processes incoming
/// messages from remote server.
struct StratumEventHandler {
    client: Arc<StratumClient>,
    all_jobs: HashMap<u32, NewMiningJob>,
    current_prevhash_msg: Option<SetNewPrevHash>,
    /// Mining target for the next job that is to be solved
    current_target: ii_bitcoin::Target,
}

impl StratumEventHandler {
    pub fn new(client: Arc<StratumClient>, current_target: ii_bitcoin::Target) -> Self {
        Self {
            client,
            all_jobs: Default::default(),
            current_prevhash_msg: None,
            current_target,
        }
    }

    /// Convert new mining job message into StratumJob and send it down the line for solving.
    ///
    /// * `job_msg` - job message used as a base for the StratumJob
    async fn update_job(&mut self, job_msg: &NewMiningJob) {
        let job = Arc::new(StratumJob::new(
            self.client.clone(),
            job_msg,
            self.current_prevhash_msg
                .as_ref()
                .expect("TODO: no prevhash"),
            self.current_target,
        ));
        self.client.update_last_job(job.clone()).await;
        self.client.job_sender.lock().await.send(job);
    }

    fn update_target(&mut self, value: Uint256Bytes) {
        let new_target: ii_bitcoin::Target = value.into();
        info!(
            "Stratum: changing target to {} diff={}",
            new_target,
            new_target.get_difficulty()
        );
        self.current_target = new_target;
    }

    async fn process_accepted_shares(&self, success_msg: &SubmitSharesSuccess) {
        let now = std::time::Instant::now();
        while let Some((solution, seq_num)) = self.client.solutions.lock().await.pop_front() {
            info!(
                "Stratum: accepted solution #{} with nonce={:08x}",
                seq_num,
                solution.nonce()
            );
            self.client
                .client_stats
                .accepted
                .account_solution(&solution.job_target(), now)
                .await;
            if success_msg.last_seq_num == seq_num {
                // all accepted solutions have been found
                return;
            }
        }
        warn!(
            "Stratum: last accepted solution #{} hasn't been found!",
            success_msg.last_seq_num
        );
    }

    async fn process_rejected_shares(&self, error_msg: &SubmitSharesError) {
        let now = std::time::Instant::now();
        while let Some((solution, seq_num)) = self.client.solutions.lock().await.pop_front() {
            if error_msg.seq_num == seq_num {
                info!(
                    "Stratum: rejected solution #{} with nonce={:08x}!",
                    seq_num,
                    solution.nonce()
                );
                self.client
                    .client_stats
                    .rejected
                    .account_solution(&solution.job_target(), now)
                    .await;
                // the rejected solution has been found
                return;
            } else {
                // TODO: this is currently not according to stratum V2 specification
                // preceding solutions are treated as accepted
                info!(
                    "Stratum: accepted solution #{} with nonce={}",
                    seq_num,
                    solution.nonce()
                );
                self.client
                    .client_stats
                    .accepted
                    .account_solution(&solution.job_target(), now)
                    .await;
                warn!(
                    "Stratum: the solution #{} precedes rejected solution #{}!",
                    seq_num, error_msg.seq_num
                );
                warn!(
                    "Stratum: the solution #{} is treated as an accepted one",
                    seq_num
                );
            }
        }
        warn!(
            "Stratum: rejected solution #{} hasn't been found!",
            error_msg.seq_num
        );
    }
}

#[async_trait]
impl Handler for StratumEventHandler {
    // The rules for prevhash/mining job pairing are (currently) as follows:
    //  - when mining job comes
    //      - store it (by id)
    //      - start mining it if it doesn't have the future_job flag set
    //  - when prevhash message comes
    //      - replace it
    //      - start mining the job it references (by job id)
    //      - flush all other jobs

    async fn visit_new_mining_job(&mut self, _header: &Header, job_msg: &NewMiningJob) {
        // all jobs since last `prevmsg` have to be stored in job table
        self.all_jobs.insert(job_msg.job_id, job_msg.clone());
        // TODO: close connection when maximal capacity of `all_jobs` has been reached

        // When not marked as future job, we can start mining on it right away (provided that we
        // already have a prevhash to work on).
        //
        // Some servers do not send `clean_jobs` flag on first `mining.notify`, which results on
        // a `NewMiningJob` being acted on immediately, which results in `no prevhash error`. This
        // should be dealt with in proxy, but let's put the `current_prevhash_msg` existence check
        // here anyway.
        if !job_msg.future_job && self.current_prevhash_msg.is_some() {
            self.update_job(job_msg).await;
        }
    }

    async fn visit_set_new_prev_hash(&mut self, _header: &Header, prevhash_msg: &SetNewPrevHash) {
        self.current_prevhash_msg.replace(prevhash_msg.clone());

        // find the future job with ID referenced in prevhash_msg
        let (_, mut future_job_msg) = self
            .all_jobs
            .remove_entry(&prevhash_msg.job_id)
            .expect("TODO: requested job ID not found");

        // remove all other jobs (they are now invalid)
        self.all_jobs.retain(|_, _| true);
        // turn the job into an immediate job
        future_job_msg.future_job = false;
        // reinsert the job
        self.all_jobs
            .insert(future_job_msg.job_id, future_job_msg.clone());

        // and start immediately solving it
        self.update_job(&future_job_msg).await;
    }

    async fn visit_set_target(&mut self, _header: &Header, target_msg: &SetTarget) {
        self.update_target(target_msg.max_target);
    }

    async fn visit_submit_shares_success(
        &mut self,
        _header: &Header,
        success_msg: &SubmitSharesSuccess,
    ) {
        self.process_accepted_shares(success_msg).await;
    }

    async fn visit_submit_shares_error(&mut self, _header: &Header, error_msg: &SubmitSharesError) {
        self.process_rejected_shares(error_msg).await;
    }
}

trait FrameSink:
    Sink<<Framing as ii_wire::Framing>::Tx, Error = futures::channel::mpsc::SendError>
    + std::marker::Unpin
    + std::fmt::Debug
    + 'static
{
    //    type Error;
}

impl<T> FrameSink for T
where
    T: Sink<<Framing as ii_wire::Framing>::Tx, Error = futures::channel::mpsc::SendError>
        + std::marker::Unpin
        + std::fmt::Debug
        + 'static,
{
    //    type Error = ??
}

trait FrameStream:
    Stream<Item = <Framing as ii_wire::Framing>::Tx> + std::marker::Unpin + 'static
{
}

impl<T> FrameStream for T where
    T: Stream<Item = <Framing as ii_wire::Framing>::Tx> + std::marker::Unpin + 'static
{
}

struct StratumSolutionHandler<S> {
    client: Arc<StratumClient>,
    connection_tx: S,
    seq_num: u32,
}

impl<S> StratumSolutionHandler<S>
where
    S: FrameSink,
    //    S: Sink<<Framing as ii_wire::Framing>::Tx, Error = E> + std::marker::Unpin + std::fmt::Debug
    //    + 'static,
    //    E: failure::Fail + std::marker::Unpin + 'static
{
    fn new(client: Arc<StratumClient>, connection_tx: S) -> Self {
        Self {
            client,
            connection_tx,
            seq_num: 0,
        }
    }

    async fn process_solution(&mut self, solution: work::Solution) -> error::Result<()> {
        let job: &StratumJob = solution.job();

        let seq_num = self.seq_num;
        self.seq_num = self.seq_num.wrapping_add(1);

        let share_msg = SubmitSharesStandard {
            channel_id: job.channel_id,
            seq_num,
            job_id: job.id,
            nonce: solution.nonce(),
            ntime: solution.time(),
            version: solution.version(),
        };
        // store solution with sequence number for future server acknowledge
        self.client
            .solutions
            .lock()
            .await
            .push_back((solution, seq_num));
        // send solutions back to the stratum server
        StratumClient::send_msg(&mut self.connection_tx, share_msg)
            .await
            .context("Cannot send submit to stratum server")?;
        // the response is handled in a separate task
        Ok(())
    }
}

struct StratumConnectionHandler {
    client: Arc<StratumClient>,
    init_target: ii_bitcoin::Target,
    status: Option<error::Result<()>>,
}

impl StratumConnectionHandler {
    pub fn new(client: Arc<StratumClient>) -> Self {
        Self {
            client,
            init_target: Default::default(),
            status: None,
        }
    }

    async fn setup_mining_connection<R, S>(
        &mut self,
        connection_rx: &mut R,
        connection_tx: &mut S,
    ) -> error::Result<()>
    where
        R: FrameStream,
        S: FrameSink,
    {
        let setup_msg = SetupConnection {
            protocol: 0,
            max_version: 2,
            min_version: 2,
            flags: 0,
            endpoint_host: Str0_255::from_string(self.client.connection_details.host.clone()),
            endpoint_port: self.client.connection_details.port,
            // TODO: Fill it with correct information
            device: DeviceInfo {
                vendor: "Braiins"
                    .try_into()
                    .expect("BUG: cannot convert 'DeviceInfo::vendor'"),
                hw_rev: "1"
                    .try_into()
                    .expect("BUG: cannot convert 'DeviceInfo::hw_rev'"),
                fw_ver: "Braiins OS 2019-06-05"
                    .try_into()
                    .expect("BUG: cannot convert 'DeviceInfo::fw_ver'"),
                dev_id: "xyz"
                    .try_into()
                    .expect("BUG: cannot convert 'DeviceInfo::dev_id'"),
            },
        };
        StratumClient::send_msg(connection_tx, setup_msg)
            .await
            .context("Cannot send stratum setup mining connection")?;
        let frame = connection_rx
            .next()
            .await
            .ok_or("The remote stratum server was disconnected prematurely")?;
        let response_msg = build_message_from_frame(frame)?;

        self.status = None;
        response_msg.accept(self).await;
        self.status.take().unwrap_or(Err(
            "Unexpected response for stratum setup mining connection".into(),
        ))
    }

    async fn open_channel<R, S>(
        &mut self,
        connection_rx: &mut R,
        connection_tx: &mut S,
    ) -> error::Result<()>
    where
        R: FrameStream,
        S: FrameSink,
    {
        let channel_msg = OpenStandardMiningChannel {
            req_id: 10,
            user: self
                .client
                .connection_details
                .user
                .clone()
                .try_into()
                .expect("BUG: cannot convert 'OpenStandardMiningChannel::user'"),
            nominal_hashrate: 1e9,
            // Maximum bitcoin target is 0xffff << 208 (= difficulty 1 share)
            max_target: ii_bitcoin::Target::default().into(),
        };

        StratumClient::send_msg(connection_tx, channel_msg)
            .await
            .context("Cannot send stratum open channel")?;
        // TODO consolidate into recv_msg in client
        let frame = connection_rx
            .next()
            .await
            .ok_or("The remote stratum server was disconnected prematurely")?;
        let response_msg = build_message_from_frame(frame)?;

        self.status = None;
        response_msg.accept(self).await;
        self.status
            .take()
            .unwrap_or(Err("Unexpected response for stratum open channel".into()))
    }

    async fn connect(self) -> error::Result<v1::Framed> {
        let socket_addr = self
            .client
            .connection_details
            .get_host_and_port()
            .to_socket_addrs()
            .context("Invalid server address")?
            // TODO: this is not correct as it always only attempts to ever connect to the first
            //  IP address from the resolved set
            .next()
            .ok_or("Cannot resolve any IP address")?;

        let connection = Connection::<v1::Framing>::connect(&socket_addr)
            .await
            .context("Cannot connect to stratum server")?;

        Ok(connection.into_inner())
    }

    /// Starts mining session and provides the initial target negotiated by the upstream endpoint
    async fn init_mining_session<R, S>(
        mut self,
        connection_rx: &mut R,
        connection_tx: &mut S,
    ) -> error::Result<ii_bitcoin::Target>
    where
        R: FrameStream,
        S: FrameSink,
    {
        self.setup_mining_connection(connection_rx, connection_tx)
            .await
            .context("Cannot setup stratum mining connection")?;
        self.open_channel(connection_rx, connection_tx)
            .await
            .context("Cannot open stratum channel")?;

        Ok(self.init_target)
    }
}

#[async_trait]
impl Handler for StratumConnectionHandler {
    async fn visit_setup_connection_success(
        &mut self,
        _header: &Header,
        _success_msg: &SetupConnectionSuccess,
    ) {
        self.status = Ok(()).into();
    }

    async fn visit_setup_connection_error(
        &mut self,
        _header: &Header,
        error_msg: &SetupConnectionError,
    ) {
        self.status =
            Err(format!("Setup connection error: {}", error_msg.code.to_string()).into()).into();
    }

    async fn visit_open_standard_mining_channel_success(
        &mut self,
        _header: &Header,
        success_msg: &OpenStandardMiningChannelSuccess,
    ) {
        self.init_target = success_msg.target.into();
        self.status = Ok(()).into();
    }

    async fn visit_open_standard_mining_channel_error(
        &mut self,
        _header: &Header,
        error_msg: &OpenStandardMiningChannelError,
    ) {
        self.status =
            Err(format!("Open channel error: {}", error_msg.code.to_string()).into()).into();
    }
}

#[derive(Debug, ClientNode)]
pub struct StratumClient {
    connection_details: ConnectionDetails,
    #[member_status]
    status: sync::StatusMonitor,
    #[member_client_stats]
    client_stats: stats::BasicClient,
    stop_sender: mpsc::Sender<()>,
    stop_receiver: Mutex<mpsc::Receiver<()>>,
    // Last job has to be week reference to prevent circular reference (the `StratumJob` keeps
    // reference to `StratumClient`)
    last_job: Mutex<Option<Weak<StratumJob>>>,
    solutions: SolutionQueue,
    job_sender: Mutex<job::Sender>,
    solution_receiver: Mutex<job::SolutionReceiver>,
}

impl StratumClient {
    const CONNECTION_TIMEOUT: time::Duration = time::Duration::from_secs(5);
    const EVENT_TIMEOUT: time::Duration = time::Duration::from_secs(60);
    const SEND_TIMEOUT: time::Duration = time::Duration::from_secs(2);

    pub fn new(connection_details: ConnectionDetails, solver: job::Solver) -> Self {
        let (stop_sender, stop_receiver) = mpsc::channel(1);
        Self {
            connection_details,
            status: Default::default(),
            client_stats: Default::default(),
            stop_sender: stop_sender,
            stop_receiver: Mutex::new(stop_receiver),
            last_job: Mutex::new(None),
            solutions: Mutex::new(VecDeque::new()),
            job_sender: Mutex::new(solver.job_sender),
            solution_receiver: Mutex::new(solver.solution_receiver),
        }
    }

    async fn update_last_job(&self, job: Arc<StratumJob>) {
        self.last_job.lock().await.replace(Arc::downgrade(&job));
    }

    /// Send a message down a specified Tx Sink
    async fn send_msg<M, S>(connection_tx: &mut S, message: M) -> error::Result<()>
    where
        M: TryInto<<Framing as ii_wire::Framing>::Tx, Error = <Framing as ii_wire::Framing>::Error>,
        S: FrameSink,
    {
        let frame = message.try_into()?;
        match connection_tx.send(frame).timeout(Self::SEND_TIMEOUT).await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(e.into()),
            Err(_) => Err("Cannot send message due to timeout")?,
        }
    }

    async fn main_loop<R, S>(
        &self,
        mut connection_rx: R,
        event_handler: &mut StratumEventHandler,
        mut solution_handler: StratumSolutionHandler<S>,
    ) -> error::Result<()>
    where
        R: FrameStream,
        S: FrameSink,
    {
        let mut solution_receiver = self.solution_receiver.lock().await;

        // while !self.status.is_shutting_down() {
        //     select! {
        //         frame = connection_rx.next().timeout(Self::EVENT_TIMEOUT).fuse() => {
        //             match frame {
        //                 Ok(Some(frame)) => {
        //                     let event_msg = build_message_from_frame(frame)?;
        //                     event_msg.accept(event_handler).await;
        //                 }
        //                 Ok(None) | Err(_) => {
        //                     Err("The remote stratum server was disconnected prematurely")?;
        //                 }
        //             }
        //         },
        //         solution = solution_receiver.receive().fuse() => {
        //             match solution {
        //                 Some(solution) => solution_handler.process_solution(solution).await?,
        //                 None => {
        //                     // TODO: initiate Destroying and remove error
        //                     Err("Standard application shutdown")?;
        //                 }
        //             }
        //         },
        //     }
        // }
        Ok(())
    }

    async fn run_job_solver<R, S>(self: Arc<Self>, mut connection_rx: R, mut connection_tx: S)
    where
        R: FrameStream,
        S: FrameSink,
    {
        // Initial target should be the result of properly initiated mining session
        let mining_session_result = StratumConnectionHandler::new(self.clone())
            .init_mining_session(&mut connection_rx, &mut connection_tx)
            .timeout(Self::CONNECTION_TIMEOUT)
            .await;
        match mining_session_result {
            Ok(Ok(init_target)) => {
                let mut event_handler = StratumEventHandler::new(self.clone(), init_target);
                let solution_handler = StratumSolutionHandler::new(self.clone(), connection_tx);
                if let Err(_) = self
                    .main_loop(connection_rx, &mut event_handler, solution_handler)
                    .await
                {
                    self.status.initiate_failing();
                }
            }
            Ok(Err(_)) | Err(_) => self.status.initiate_failing(),
        }
    }

    async fn run(self: Arc<Self>) {
        match StratumConnectionHandler::new(self.clone())
            .connect()
            .timeout(Self::CONNECTION_TIMEOUT)
            .await
        {
            Ok(Ok(v1_framed_connection)) => {
                if self.status.initiate_running() {
                    let options = V2ToV1TranslationOptions {
                        try_enable_xnsub: self.connection_details.try_enable_xnsub(),
                    };
                    let (translation_handler, v2_translation_rx, v2_translation_tx) =
                        TranslationHandler::new(v1_framed_connection, options);
                    tokio::spawn(async move {
                        let status = translation_handler.run().await;
                        info!("V2->V1 translation terminated: {:?}", status);
                    });
                    self.clone()
                        .run_job_solver(v2_translation_rx, v2_translation_tx)
                        .await;
                }
            }
            Ok(Err(_)) | Err(_) => self.status.initiate_failing(),
        }
    }

    async fn main_task(self: Arc<Self>) {
        // TODO: Count as a discarded solution?
        // Flush all obsolete solutions from previous run
        self.solution_receiver.lock().await.flush();

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
        //     // Flush all unprocessed solutions to empty buffer
        //     // TODO: Count as a discarded solution?
        //     self.solution_receiver.lock().await.flush();
        //     self.solutions.lock().await.clear();
        //
        //     if self.status.can_stop() {
        //         // NOTE: it is not safe to add here any code!
        //         // The reason is that at this point the main task can be executed in parallel again
        //         break;
        //     }
        //     // Restarting
        // }
    }
}

/// This object receives V1 messages and passes them to `V2ToV1Translation` component for
/// translation. The user of this component is provided with an Rx/Tx channel pair that is
/// intended for sending V2 messages and receiving the translated V2 messages.
/// V1 messages received from the translator are sent out via V1 connection.
struct TranslationHandler {
    /// Actual protocol translator
    translation: V2ToV1Translation,
    /// Upstream V1 connection
    v1_conn: v1::Framed,
    /// Receiver for V1 frames from the translator that will be sent out via V1 connection
    v1_translation_rx: mpsc::Receiver<v1::Frame>,
    /// V2 Frames from the client that we use for feeding the translator
    v2_client_rx: mpsc::Receiver<v2::Frame>,
}

impl TranslationHandler {
    const MAX_TRANSLATION_CHANNEL_SIZE: usize = 10;

    /// Builds the new translation handler and provides Tx/Rx communication ends
    fn new(
        v1_conn: v1::Framed,
        options: V2ToV1TranslationOptions,
    ) -> (Self, mpsc::Receiver<v2::Frame>, mpsc::Sender<v2::Frame>) {
        let (v1_translation_tx, v1_translation_rx) =
            mpsc::channel(Self::MAX_TRANSLATION_CHANNEL_SIZE);
        let (v2_translation_tx, v2_translation_rx) =
            mpsc::channel(Self::MAX_TRANSLATION_CHANNEL_SIZE);
        let (v2_client_tx, v2_client_rx) = mpsc::channel(Self::MAX_TRANSLATION_CHANNEL_SIZE);

        let translation = V2ToV1Translation::new(v1_translation_tx, v2_translation_tx, options);

        (
            Self {
                translation,
                v1_conn,
                v1_translation_rx,
                v2_client_rx,
            },
            v2_translation_rx,
            v2_client_tx,
        )
    }

    /// Executive part of the translation handler that drives the translation component and acts
    /// like a message pump between the actual V2 client, translation component and upstream V1
    /// server.
    /// The main task selects from the following events and performs corresponding acction
    /// - v1_conn_rx -> build message + accept(translation)
    /// - v2_client_rx -> build message + accept(translation)
    /// - v1_translation_rx -> send
    /// terminate upon any error or timeout
    async fn run(mut self) -> error::Result<()> {
        unimplemented!()
        //while !self.status.is_shutting_down() {
        // info!("Starting V2->V1 translation handler");
        // loop {
        //     select! {
        //         // Receive V1 frame and translate it to V2 message
        //         v1_frame = self.v1_conn.next().timeout(StratumClient::EVENT_TIMEOUT).fuse() => {
        //             match v1_frame {
        //                 Ok(Some(v1_frame)) => {
        //                     let v1_msg = v1::build_message_from_frame(v1_frame?)?;
        //                     v1_msg.accept(&mut self.translation).await;
        //                 }
        //                 Ok(None) | Err(_) => {
        //                     Err("Upstream V1 stratum connection dropped terminating translation")?;
        //                 }
        //             }
        //         },
        //         // Receive V2 frame from our client (no timeout needed) and pass it to V1
        //         // translation
        //         v2_frame = self.v2_client_rx.next().fuse() => {
        //             match v2_frame {
        //                 Some(v2_frame) => {
        //                     let v2_msg = v2::build_message_from_frame(v2_frame)?;
        //                     v2_msg.accept(&mut self.translation).await;
        //                 }
        //                 None => {
        //                     Err("V2 client shutdown, terminating translation")?;
        //                 }
        //             }
        //         },
        //         // Receive V1 frame from the translation and send it upstream
        //         v1_frame = self.v1_translation_rx.next().fuse() => {
        //             match v1_frame {
        //                 Some(v1_frame) => self
        //                     .v1_conn
        //                     .send(v1_frame)
        //                     // NOTE: this timeout is important otherwise the whole task could
        //                     // block indefinitely and the above timeout for v1_conn_rx wouldn't
        //                     // do anything. Besides this, we don't want to wait with system time
        //                     // out in case the upstream connection just hangs
        //                     .timeout(StratumClient::EVENT_TIMEOUT)
        //                     .await
        //                     // Unwrap timeout and actual sending error
        //                     .map_err(|e| "V1 send timeout")??,
        //                 None => {
        //                     Err("V1 translation component terminated, terminating translation")?;
        //                 }
        //             }
        //         },
        //     }
        // }
    }
}

#[async_trait]
impl node::Client for StratumClient {
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
        self.last_job
            .lock()
            .await
            .as_ref()
            .and_then(|job| job.upgrade().map(|job| job as Arc<dyn job::Bitcoin>))
    }
}

impl fmt::Display for StratumClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}://{}@{}",
            ClientProtocol::SCHEME_STRATUM_V1,
            self.connection_details.host,
            self.connection_details.user
        )
    }
}
