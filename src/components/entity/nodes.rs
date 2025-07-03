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
    fn start(&mut self, port: u16) {
        // Use for communicating among threads inside node
        let (sndr_r2p, rcvr_r2p) = channel::<Packet>();
        let (sndr_p2s, rcvr_p2s) = channel::<Packet>();
        // ================================================
        // Declare different threads for different functions
        // ================================================
        let flag_stop = Arc::new(AtomicBool::new(false));

        let thread_rcvr = match self.create_thread_receiver(port, sndr_r2p, &flag_stop) {
            Ok(handle) => handle,
            Err(err) => {
                log::error!("{}", err);
                return;
            }
        };
        let thread_sndr = match self.create_thread_sender(rcvr_p2s, &flag_stop) {
            Ok(handle) => handle,
            Err(err) => {
                log::error!("{}", err);
                return;
            }
        };

        // ================================================
        // Start processing packets
        // ================================================
        if let Err(_) = self.trigger_processor(&rcvr_r2p, &sndr_p2s) {
            self.trigger_graceful_shutdown(&flag_stop, port, &sndr_p2s);
        }

        // ================================================
        // Join threads
        // ================================================
        self.trigger_graceful_shutdown(&flag_stop, port, &sndr_p2s);

        if let Err(err) = thread_rcvr.join() {
            log::error!("Error as creating thread_rcvr: {:?}", err);
            return;
        }
        if let Err(err) = thread_sndr.join() {
            log::error!("Error as creating thread_sndr: {:?}", err);
            return;
        }
    }

    /// Create a thread dedicated for receiving incoming message
    fn create_thread_receiver(
        &self,
        port: u16,
        sndr_r2p: Sender<Packet>,
        flag_stop: &Arc<AtomicBool>,
    ) -> Result<JoinHandle<()>, NodeCreationError> {
        log::info!("Creating thread: Receiver");

        let flag = Arc::clone(&flag_stop);
        let addr_node = SocketAddr::from(([0, 0, 0, 0], port));

        Ok(thread::spawn(move || {
            let listener = match TcpListener::bind(&addr_node) {
                Ok(listener) => listener,
                Err(_) => {
                    log::error!("Cannot bind to {}", addr_node);
                    panic!();
                }
            };
            log::info!("Server starts at {}", addr_node);

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
                    Err(_) => {
                        log::error!("Cannot connect to address: {}", addr_rcv);
                        continue;
                    }
                };

                let a = packet.to_bytes();
                if let Err(err) = stream.write_all(a.as_slice()) {
                    log::error!("Cannot send to address: {} : {}", &addr_rcv, err);
                }
            }
        }))
    }

    /// Gracefully shutdown thread:Receiver and thread:Sender
    fn trigger_graceful_shutdown(&self, flag_stop: &Arc<AtomicBool>, port: u16, sndr_p2s: &Sender<Packet>) {
        log::debug!("trigger_graceful_shutdown invoked!");

        flag_stop.store(true, Ordering::Relaxed);

        // Shutdown thread:Receiver
        if let Err(err) = TcpStream::connect(SocketAddr::from(([127, 0, 0, 1], port))) {
            log::error!("Error as executing gracefull shutdown: {}", err);
        };

        // Shutdown thread:Sender
        forward_packet(
            sndr_p2s,
            Packet::create_heartbeat(SocketAddr::from(([127, 0, 0, 1], port))),
        );
    }

    /// Start processor
    fn trigger_processor(
        &mut self,
        rcvr_r2p: &Receiver<Packet>,
        sndr_p2s: &Sender<Packet>,
    ) -> Result<(), NodeCreationError>;
}
