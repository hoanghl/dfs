# Gracefully shutdowning nodes and clients

Each node (Master, Data) and client hold 3 threads:

- Processor thread: Handle every logic
- Receiver thread: Receive packets sent from other nodes/clients
- Sender thread: Deliver packets crafted by Processor thread

Graceful shutdown is triggered by Processor thread. A flag is shared among 3 threads to check whether shutdown is begun. In parallel, each of Sender and Receiver thread employ different methods to execute shutdown process.

Denote:

- **P**: Processor thread
- **R**: Receiver thread
- **S**: Sender thread
- **F**: Shutdown flag shared among threads. Only Processor thread can modify this flag whereas other threads will read this flag.

## 1. Graceful shutdown on Receiver thread

The main loop of this thread is receiving incoming packet with `listener.incoming()`, so shutdown process happens as follow.

1. **P** set **F** to true
2. **P** tries to connect to **R** by connect with `TcpStream::connect`
3. Connection from **P** wakes up **R** from loop `listener.incoming()`, **R** reads **F** and breaks the loop

## 2. Gracefull shutdown on Sender thread

The main loop is infinite loop and in the begining of loop body, Sender thread checks with timeout the availability of packet sent via channel by Processor thread. Therefore, no further action from Processor thread is needed except setting flag to True.

The shutdown procedure, hence, happens as follow.

1. **P** set **F** to true
2. **R** reads **F** at the begining of loop body, and breaks the loop
