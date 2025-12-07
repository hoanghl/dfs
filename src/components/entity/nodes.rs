use log;

use std::{
    io::Write,
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{channel, Receiver, Sender},
        Arc,
    },
    thread::{self, JoinHandle},
};

use crate::components::{
    errors::NodeCreationError,
    packets::{forward_packet, Packet},
};

// ================================================
// Definition
// ================================================

pub trait Node {
    fn start(&mut self, addr_local: &SocketAddr) {
        // Use for communicating among threads inside node
        let (sndr_r2p, rcvr_r2p) = channel::<Packet>();
        let (sndr_p2s, rcvr_p2s) = channel::<Packet>();
        // ================================================
        // Declare different threads for different functions
        // ================================================
        let flag_stop = Arc::new(AtomicBool::new(false));

        let thread_rcvr = match self
            .create_thread_receiver(addr_local, sndr_r2p, &flag_stop)
        {
            Ok(handle) => handle,
            Err(e) => {
                panic!("Err: {}", e);
            }
        };
        let thread_sndr = match self.create_thread_sender(rcvr_p2s, &flag_stop)
        {
            Ok(handle) => handle,
            Err(err) => {
                panic!("Err: {}", err);
            }
        };

        // ================================================
        // Start processing packets
        // ================================================
        if let Err(e) = self.trigger_processor(&rcvr_r2p, &sndr_p2s) {
            self.trigger_graceful_shutdown(&flag_stop, addr_local, &sndr_p2s);
            log::error!("Err: {}", e);
        };

        log::info!("thread:Processor is stopped");

        // ================================================
        // Join threads
        // ================================================
        self.trigger_graceful_shutdown(&flag_stop, addr_local, &sndr_p2s);

        log::info!("Trigger stop for thread:Sender and thread:Receiver");

        if let Err(err) = thread_rcvr.join() {
            log::error!("Error as creating thread_rcvr: {:?}", err);
        }
        if let Err(err) = thread_sndr.join() {
            log::error!("Error as creating thread_sndr: {:?}", err);
        }

        log::info!("thread:Sender and thread:Receiver stopped");
    }

    /// Create a thread dedicated for receiving incoming message
    fn create_thread_receiver(
        &self,
        addr_local: &SocketAddr,
        sndr_r2p: Sender<Packet>,
        flag_stop: &Arc<AtomicBool>,
    ) -> Result<JoinHandle<()>, NodeCreationError> {
        log::info!("Creating thread: Receiver");

        let flag = Arc::clone(&flag_stop);

        let addr_local = addr_local.clone();

        Ok(thread::spawn(move || {
            let listener = match TcpListener::bind(addr_local) {
                Ok(listener) => listener,
                Err(err) => {
                    log::error!("Cannot bind to {}: {}", addr_local, err);
                    panic!();
                }
            };
            log::info!("Server starts at {}", addr_local);

            for stream in listener.incoming() {
                if flag.load(Ordering::Relaxed) {
                    break;
                }

                match stream {
                    Ok(mut stream) => {
                        let packet = match Packet::from_stream(&mut stream) {
                            Ok(packet) => packet,
                            Err(e) => {
                                log::error!("{}", e);
                                continue;
                            }
                        };

                        log::debug!(
                            "Receive connection from: {:?}: {:?}",
                            stream.peer_addr(),
                            packet.packet_id
                        );

                        // Send to thread Processor
                        if let Err(err) = sndr_r2p.send(packet) {
                            log::error!(
                                "Error as sending packet from thread:Receiver -> thread:Processor: err = {}",
                                err
                            );
                        };
                    }
                    Err(e) => {
                        log::error!("{}", e);
                        continue;
                    }
                }
            }
        }))
    }

    /// Create thread for sending packet
    fn create_thread_sender(
        &mut self,
        rcvr_p2s: Receiver<Packet>,
        flag_stop: &Arc<AtomicBool>,
    ) -> Result<JoinHandle<()>, NodeCreationError> {
        log::info!("Creating thread: Sender");

        let flag = Arc::clone(&flag_stop);
        Ok(thread::spawn(move || {
            for packet in rcvr_p2s {
                if flag.load(Ordering::Relaxed) {
                    break;
                }

                let addr_rcv = match packet.addr_rcv {
                    Some(addr) => addr,
                    None => {
                        log::error!("Field 'addr_rcv' not specified.");
                        continue;
                    }
                };

                // Connect and send
                let mut stream = match TcpStream::connect(&addr_rcv) {
                    Ok(stream) => stream,
                    Err(err) => {
                        log::error!(
                            "Cannot connect to address: {}: {}",
                            addr_rcv,
                            err
                        );
                        continue;
                    }
                };

                let a = packet.to_bytes();
                if let Err(err) = stream.write_all(a.as_slice()) {
                    log::error!(
                        "Cannot send to address: {} : {}",
                        &addr_rcv,
                        err
                    );
                }
            }
        }))
    }

    /// Gracefully shutdown thread:Receiver and thread:Sender
    fn trigger_graceful_shutdown(
        &self,
        flag_stop: &Arc<AtomicBool>,
        addr_local: &SocketAddr,
        sndr_p2s: &Sender<Packet>,
    ) {
        log::debug!("trigger_graceful_shutdown invoked!");

        flag_stop.store(true, Ordering::Relaxed);

        // Shutdown thread:Receiver
        if let Err(err) = TcpStream::connect(addr_local) {
            log::error!("Error as executing gracefull shutdown: {}", err);
        };

        // Shutdown thread:Sender
        forward_packet(
            sndr_p2s,
            Packet::create_heartbeat(addr_local.clone(), addr_local.port()),
        );
    }

    /// Start processor
    fn trigger_processor(
        &mut self,
        rcvr_r2p: &Receiver<Packet>,
        sndr_p2s: &Sender<Packet>,
    ) -> Result<(), NodeCreationError>;
}
