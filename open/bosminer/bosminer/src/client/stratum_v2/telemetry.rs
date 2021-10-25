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

use crate::error::{self, ResultExt};

use async_trait::async_trait;
use bytes::BytesMut;
use futures::channel::mpsc;

use ii_async_compat::prelude::*;
use ii_async_compat::{bytes, select};
use ii_logging::macros::*;
use ii_stratum::v2::{self, extensions, framing, telemetry::messages::*, types::*};

use super::ExtensionChannelMsg;

/// Make channel ID type more visible in the code
type ChannelId = u32;

/// TODO consider transforming the code below into a state pattern instead of multiplexing the
/// states all the time
#[derive(Debug)]
enum State {
    /// Telemetry Channel not open yet
    Init,
    /// Handshake started (we have sent OpenTelemetryChannel)
    Handshake,
    /// Operational state is associated with channel ID assigned to the client by the server and
    /// will be used through out the communication
    Operational(ChannelId),
}

/// Client for telemetry stratum extension receives raw telemetry data from all components (e.g.
/// hashboards) and submits it via the stratum telemetry extension protocol.  The main idea is
/// that the telemetry client should never terminate based on protocol errors. It can only be
/// terminated based on explicit closing of communication channels from the main Stratum V2 client.
#[derive(Debug)]
pub struct Client {
    state: State,

    /// Receive control commands and telemetry extension messages
    /// TODO: consider replacing this channel with a standard API -> start/stop/handle_frame()
    stratum_receiver: super::ExtensionChannelFromStratumReceiver,
    /// Channel to send telemetry extension messages
    stratum_sender: super::ExtensionChannelToStratumSender,

    /// Raw telemetry data being received from all components that were given the
    /// `telem_data_sender` endpoint
    telem_data_receiver: mpsc::UnboundedReceiver<BytesMut>,
    /// Sender endpoint that this client provides to any component that is interested in
    /// submitting the telemetry data
    telem_data_sender: mpsc::UnboundedSender<BytesMut>,

    /// Current request ID/sequence ID.
    curr_request_id: u32,

    /// Current telemetry submission sequence ID.
    curr_data_sequence_id: u32,
    /// Device ID that will be used when creating the telemetry channel
    dev_id: Str0_255,
}

impl Client {
    const CHANNEL_CAPACITY: usize = 16;

    /// Creates a new client and provides the communication endpoints for it
    pub fn new(
        dev_id: String,
    ) -> (
        Self,
        super::ExtensionChannelToStratumReceiver,
        super::ExtensionChannelFromStratumSender,
    ) {
        // Prepare the communication channels between stratum client and the telemetry extension
        let (from_stratum_sender, from_stratum_receiver) = mpsc::channel(Self::CHANNEL_CAPACITY);
        let (to_stratum_sender, to_stratum_receiver) = mpsc::channel(Self::CHANNEL_CAPACITY);

        let (telem_data_sender, telem_data_receiver) = mpsc::unbounded();

        let client = Self {
            state: State::Init,
            stratum_receiver: from_stratum_receiver,
            stratum_sender: to_stratum_sender,
            telem_data_sender,
            telem_data_receiver,
            curr_request_id: 0,
            curr_data_sequence_id: 0,
            dev_id: dev_id.try_into().expect("TODO: dev ID cannot be converted"),
        };

        (client, to_stratum_receiver, from_stratum_sender)
    }

    pub async fn run(mut self) -> error::Result<()> {
        unimplemented!()
        // loop {
        //     select! {
        //         message = self.stratum_receiver.next().fuse() => {
        //             match message {
        //                 Some(message) => {
        //                     self.handle_message(message).await?
        //                 }
        //                 None => {
        //                     Err("The remote endpoint stopped")?;
        //                 }
        //             }
        //         }
        //         // Wrap telemetry data and send it upstream
        //         data = self.telem_data_receiver.next().fuse() => {
        //             let data = data.ok_or("End of telemetry stream")?;
        //             self.send_telemetry(data).await?;
        //         }
        //     }
        // }
    }

    pub fn get_unbounded_sender(&self) -> mpsc::UnboundedSender<BytesMut> {
        self.telem_data_sender.clone()
    }

    ///
    async fn handle_message(&mut self, message: ExtensionChannelMsg) -> error::Result<()> {
        match message {
            ExtensionChannelMsg::Start => self.start_channel().await,
            // TODO currently there is no channel close protocol. This may need to be improved
            ExtensionChannelMsg::Stop => {
                self.state = State::Init;
                Ok(())
            }
            ExtensionChannelMsg::Frame(frame) => self.handle_frame(frame).await,
        }
    }

    /// Submits data when in operational state, ignores the data in any other state.
    /// TODO: consider queueing the data to some level
    async fn send_telemetry(&mut self, data: BytesMut) -> error::Result<()> {
        match self.state {
            State::Operational(channel_id) => {
                let msg = SubmitTelemetryData {
                    channel_id,
                    seq_num: self.next_data_sequence_id(),
                    // TODO investigate why stratum TryFrom<Bytes0_64k> returns () as error variant
                    //  see
                    telemetry_payload: data[..]
                        .try_into()
                        .map_err(|e| format!("Invalid telemetry data to serialize {:?}", e))?,
                };
                self.send_msg(msg).await
            }
            _ => {
                // Telemetry cannot be sent in any other state. We will ignore the data for the time
                // being. However, we will not communicate the error as we don't want to break
                // the possibly ongoing handshake stage
                self.log_error("Cannot send telemetry, ignoring the data");
                Ok(())
            }
        }
    }

    async fn handle_frame(&mut self, frame: framing::Frame) -> error::Result<()> {
        assert_eq!(
            frame.header.extension_type,
            extensions::TELEMETRY,
            "BUG: unexpected extension"
        );

        let telemetry_msg = build_message_from_frame(frame)?;
        telemetry_msg.accept(self).await;
        Ok(())
    }

    async fn start_channel(&mut self) -> error::Result<()> {
        match self.state {
            State::Init => {
                self.state = State::Handshake;
                let msg = OpenTelemetryChannel {
                    req_id: self.next_request_id(),
                    dev_id: self.dev_id.clone(),
                };
                self.log_info(format!("starting client, message: {:?}", msg).as_str());
                self.send_msg(msg).await
            }
            _ => {
                let err_msg = "Cannot start telemetry client";
                self.log_error(err_msg);
                Err(error::ErrorKind::Stratum(err_msg.to_string()).into())
            }
        }
    }

    async fn send_msg<M>(&mut self, message: M) -> error::Result<()>
    where
        M: TryInto<
            <framing::Framing as ii_wire::Framing>::Tx,
            Error = <framing::Framing as ii_wire::Framing>::Error,
        >,
    {
        let frame = message.try_into()?;

        self.stratum_sender
            .try_send(frame)
            .context("submit message")
            .map_err(Into::into)
    }

    /// Helper that logs about an error appending the current telemetry state
    fn log_info(&self, info_msg: &str) {
        let info_msg = format!("Telemetry: {}, state: {:?}", info_msg, self.state);
        info!("{}", info_msg);
    }

    /// Helper that logs about an error appending the current telemetry state
    fn log_error(&self, err_msg: &str) {
        let err_msg = format!("Telemetry: {}, state: {:?}", err_msg, self.state);
        error!("{}", err_msg);
    }

    /// Helper that generates a request ID mismatch error based on `received_req_id`
    fn log_error_request_id(&self, err_msg: &str, received_req_id: u32) {
        let err_msg = format!(
            "{} Request ID mismatch - expected: {}, received: {}",
            err_msg, self.curr_request_id, received_req_id
        );
        self.log_error(err_msg.as_str());
    }

    /// Helper that generates a channel ID mismatch error based on `received_req_id`
    fn log_error_channel_id(
        &self,
        err_msg: &str,
        expected_channel_id: u32,
        received_channel_id: u32,
    ) {
        let err_msg = format!(
            "{}, Channel id mismatch - expected: {}, received: {}",
            err_msg, expected_channel_id, received_channel_id
        );
        self.log_error(err_msg.as_str());
    }

    /// Generates a next request ID and returns its next value. This also implies that the first ID
    /// generated is 1
    fn next_request_id(&mut self) -> u32 {
        self.curr_request_id = self.curr_request_id.wrapping_add(1);
        self.curr_request_id
    }

    /// Generates a data submission sequence ID and returns its next value. This also implies
    /// that the first ID generated is 1
    fn next_data_sequence_id(&mut self) -> u32 {
        self.curr_data_sequence_id = self.curr_data_sequence_id.wrapping_add(1);
        self.curr_data_sequence_id
    }
}

#[async_trait]
impl v2::Handler for Client {
    async fn visit_open_telemetry_channel_success(
        &mut self,
        _header: &framing::Header,
        payload: &OpenTelemetryChannelSuccess,
    ) {
        match self.state {
            State::Handshake => {
                if payload.req_id == self.curr_request_id {
                    self.state = State::Operational(payload.channel_id);
                    self.log_info("channel operational");
                    self.next_request_id();
                } else {
                    self.log_error_request_id("OpenTelemetryChannelSuccess", payload.req_id);
                    self.state = State::Init;
                }
            }
            _ => {
                self.log_error("Unexpected OpenTelemetryChannelSuccess message");
            }
        };
    }

    async fn visit_open_telemetry_channel_error(
        &mut self,
        _header: &framing::Header,
        payload: &OpenTelemetryChannelError,
    ) {
        match self.state {
            State::Handshake => {
                if payload.req_id == self.curr_request_id {
                    self.state = State::Init;
                    info!(
                        "Failed to open telemetry channel code: {}, state: {:?}",
                        payload.code.to_string(),
                        self.state
                    );
                    self.next_request_id();
                } else {
                    self.log_error_request_id("OpenTelemetryChannelError", payload.req_id);
                }
            }
            _ => {
                self.log_error("Unexpected OpenTelemetryMessageError message");
            }
        };

        // Error opening the channel moves the statemachine into an initial state
        self.state = State::Init;
    }

    async fn visit_submit_telemetry_data_success(
        &mut self,
        _header: &framing::Header,
        payload: &SubmitTelemetryDataSuccess,
    ) {
        match self.state {
            State::Operational(channel_id) => {
                if payload.channel_id == channel_id {
                    self.log_info(
                        format!("data confirmed, last_seq_num ID: {}", payload.last_seq_num)
                            .as_str(),
                    );
                } else {
                    self.log_error_channel_id(
                        "SubmitTelemetryDataSuccess",
                        channel_id,
                        payload.channel_id,
                    );
                }
            }
            _ => {
                self.log_error("Unexpected SubmitTelemetryDataSuccess message");
            }
        }
    }

    async fn visit_submit_telemetry_data_error(
        &mut self,
        _header: &framing::Header,
        payload: &SubmitTelemetryDataError,
    ) {
        match self.state {
            State::Operational(channel_id) => {
                if payload.channel_id == channel_id {
                    self.log_info(
                        format!("data rejected sequence seq_num: {}", payload.seq_num).as_str(),
                    );
                } else {
                    self.log_error_channel_id(
                        "SubmitTelemetryDataError",
                        channel_id,
                        payload.channel_id,
                    );
                }
            }
            _ => {
                self.log_error("Unexpected SubmitTelemetryDataError message");
            }
        }
    }
}
