"""
Headless Service for Universal Connectivity Python Peer

This module provides a headless service that manages libp2p host, pubsub, and chat functionality
without any UI. It communicates with the UI through queues and events.
"""

import logging
import random
import socket
import time
import traceback
import multiaddr
import janus
import trio
import trio_asyncio
import hashlib
from queue import Empty
from typing import List, Dict, Any, Set
from libp2p.discovery.bootstrap import BootstrapDiscovery
from libp2p.kad_dht.kad_dht import (
    DHTMode,
    KadDHT,
)
from libp2p import new_host
from libp2p.crypto.rsa import create_new_key_pair
from libp2p.pubsub.gossipsub import GossipSub
from libp2p.pubsub.pubsub import Pubsub
from libp2p.tools.async_service.trio_service import background_trio_service
from libp2p.peer.peerinfo import info_from_p2p_addr
from libp2p.peer.peerinfo import PeerInfo
from libp2p.identity.identify.identify import identify_handler_for, parse_identify_response, ID as IDENTIFY_PROTOCOL_ID
from libp2p.utils.varint import read_length_prefixed_protobuf
from libp2p.peer.id import ID
from libp2p.custom_types import TProtocol
from libp2p.pubsub.gossipsub import PROTOCOL_ID, PROTOCOL_ID_V11
from libp2p.protocol_muxer.exceptions import (
    MultiselectClientError,
)
from libp2p.host.exceptions import (
    StreamFailure,
)

from libp2p.host.autonat.autonat import AutoNATService 
from libp2p.relay.circuit_v2.protocol import CircuitV2Protocol 
from libp2p.relay.circuit_v2.discovery import RelayDiscovery
from libp2p.relay.circuit_v2.dcutr import DCUtRProtocol

from chatroom import ChatRoom, ChatMessage

logger = logging.getLogger("headless")

# Constants
DISCOVERY_SERVICE_TAG = "universal-connectivity"
PROTOCOL_ID_LIST = [PROTOCOL_ID]
# PROTOCOL_ID_V11 - giving a problem when using docker with py-libp2p nodes, so commenting out for now to maintain compatibility. Will investigate further.
DEFAULT_PORT = 9095

# Bootstrap nodes for peer discovery
BOOTSTRAP_PEERS = [
    # "/ip4/139.178.65.157/tcp/4001/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
    # "/ip4/139.178.91.71/tcp/4001/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
    # "/ip4/145.40.118.135/tcp/4001/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt"
    # "/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
    # "/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa", 
    # "/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zp7ykQCj2gRNdrFeqQ1vG13rMb4sPS",
    # "/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
    # "/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ"
]


def find_free_port() -> int:
    """Find a free port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))  # Bind to a free port provided by the OS
        return s.getsockname()[1]

def filter_compatible_peer_info(peer_info) -> bool:
    """Filter peer info to check if it has compatible addresses (TCP + IPv4)."""
    if not hasattr(peer_info, "addrs") or not peer_info.addrs:
        return False

    for addr in peer_info.addrs:
        addr_str = str(addr)
        if "/tcp/" in addr_str and "/ip4/" in addr_str and "/quic" not in addr_str:
            return True
    return False

async def maintain_connections(host) -> None:
    """Maintain connections to ensure the host remains connected to healthy peers."""
    while True:
        try:
            connected_peers = host.get_connected_peers()
            list_peers = host.get_peerstore().peers_with_addrs()

            if len(connected_peers) < 20:
                logger.debug("Reconnecting to maintain peer connections...")

                # Find compatible peers
                compatible_peers = []
                for peer_id in list_peers:
                    try:
                        peer_info = host.get_peerstore().peer_info(peer_id)
                        if filter_compatible_peer_info(peer_info):
                            compatible_peers.append(peer_id)
                    except Exception:
                        continue

                # Connect to random subset of compatible peers
                if compatible_peers:
                    random_peers = random.sample(
                        compatible_peers, min(50, len(compatible_peers))
                    )
                    for peer_id in random_peers:
                        if peer_id not in connected_peers:
                            try:
                                with trio.move_on_after(5):
                                    peer_info = host.get_peerstore().peer_info(peer_id)
                                    await host.connect(peer_info)
                                    logger.debug(f"Connected to peer: {peer_id}")
                            except Exception as e:
                                logger.debug(f"Failed to connect to {peer_id}: {e}")

            await trio.sleep(15)
        except Exception as e:
            logger.error(f"Error maintaining connections: {e}")


class HeadlessService:
    """
    Headless service that manages libp2p components and provides data to UI through queues.
    """

    def __init__(self, nickname: str, port: int = 0, connect_addrs: List[str] = None, ui_mode: bool = False, strict_signing: bool = True, seed: int = None, topic: str = None, relay_addrs: List[str] = None, relay_server_mode: bool = True, enable_autonat: bool = True, enable_dcutr: bool = True):
        self.nickname = nickname
        self.port = port if port != 0 else find_free_port()
        self.connect_addrs = connect_addrs or []
        self.ui_mode = ui_mode  # Flag to control logging behavior
        self.strict_signing = strict_signing  # Flag to control message signing
        self.seed = seed
        self.topic = topic  # Custom topic to use instead of default

        # NAT traversal config 

        self.relay_addrs : List[str] = relay_addrs or []
        self.relay_server_mode : bool = relay_server_mode
        self.enable_autonat : bool = enable_autonat
        self.enable_dcutr : bool = enable_dcutr

        self.autonat: AutoNATService = None          # AutoNATService instance
        self.circuit_v2: CircuitV2Protocol = None    # CircuitV2Protocol instance
        self.relay_discovery:RelayDiscovery = None   # RelayDiscovery instance
        self.dcutr: DCUtRProtocol = None             # DCUtRProtocol instance

        # libp2p components
        self.host = None
        self.pubsub = None
        self.gossipsub = None
        self.dht = None
        self.chat_room = None
        
        # Service state
        self.running = False
        self.ready = False
        self.full_multiaddr = None
        
        # Communication with UI
        self.message_queue = None  # UI receives messages from headless
        self.system_queue = None   # UI receives system messages from headless
        self.outgoing_queue = None # UI sends messages to headless
        self.topic_subscription_queue = None  # UI sends topic subscription requests
        self.peer_connection_queue = None  # UI sends peer connection requests
        
        # Per-topic message storage
        self.topic_messages = {}  # {topic: [{'message': msg, 'timestamp': ts, 'read': bool}]}
        self.topic_unread_counts = {}  # {topic: int}
        
        # Peer information storage for identify protocol
        self.peer_info_cache = {}  # Store peer info retrieved through identify
        
        # Events for synchronization
        self.ready_event = trio.Event()
        self.stop_event = trio.Event()
        
        if not ui_mode:
            logger.info(
                f"HeadlessService init — nickname: {nickname}, port: {self.port}, "
                f"strict_signing: {strict_signing}, relay_server_mode: {relay_server_mode}, "
                f"relay_addrs: {self.relay_addrs}, autonat: {enable_autonat}, dcutr: {enable_dcutr}"
            )
    
    async def monitor_peers(self):
        while True:
            print("testing print")
            logger.info("testing status")
            logger.info(f"Connected peers are: len{self.host.get_connected_peers()}")
            logger.info(f"peers in peer store are: len{self.host.get_peerstore().peers_with_addrs()}")
            logger.info(f"peers in routing table are: len{self.dht.routing_table.get_peer_ids()}")
            logger.info(f"peers in pubsub are: {(self.pubsub.peers.keys())}")
            await trio.sleep(5)

    async def start(self):
        """Start the headless service."""
        logger.info("Starting headless service...")
        
        try:
            # Create queues for communication with UI
            logger.debug("Creating message queues...")
            self.message_queue = janus.Queue()      # Messages from headless to UI
            self.system_queue = janus.Queue()       # System messages from headless to UI  
            self.outgoing_queue = janus.Queue()     # Messages from UI to headless
            self.topic_subscription_queue = janus.Queue()  # Topic subscription requests from UI
            self.peer_connection_queue = janus.Queue()  # Peer connection requests from UI
            logger.debug("Message queues created successfully")
            
            # Enable trio-asyncio mode
            async with trio_asyncio.open_loop():
                # Send initial system message to test queue inside trio context
                await self._send_system_message("Headless service starting...")
                await self._run_service()
                    
        except Exception as e:
            logger.error(f"Failed to start headless service: {e}")
            logger.error(f"Traceback:\n{traceback.format_exc()}")
            raise
    
    async def _run_service(self):
        """Run the main service loop."""
        key_pair = create_new_key_pair()
        
        # Create listen address
        listen_addr = multiaddr.Multiaddr(f"/ip4/0.0.0.0/tcp/{self.port}")
        
        # Create libp2p host WITHOUT bootstrap nodes initially
        # We'll connect to bootstrap nodes after pubsub is running
        self.host = new_host(
            key_pair=key_pair
            # bootstrap = BOOTSTRAP_PEERS
        )

        # configure AutoNAT if enabled 
        if self.enable_autonat:
            try:
                self.autonat = AutoNATService(self.host)
                logger.info("AutoNAT service created successfully")
            except Exception as e: 
                logger.warning(f"AutoNAT service could not be created: {e}")

        # Configure relay if enabled
        if self.relay_server_mode or self.relay_addrs:
            try:
                self.circuit_v2 = CircuitV2Protocol(
                    host=self.host,
                    allow_hop=self.relay_server_mode,
                )
                mode_label = "HOP server" if self.relay_server_mode else "STOP client"
                logger.info(f"CircuitV2Protocol created ({mode_label} mode)")
                await self._send_system_message(
                    f"circuit_v2 created, allow_hop={self.relay_server_mode}, mode={mode_label}"
                )
            except Exception as e:
                logger.warning(f"CircuitV2Protocol init failed (non-fatal): {e}")
        
        # Setup relay discovery if relay addresses are provided
        if self.relay_addrs:
            try:
                self.relay_discovery = RelayDiscovery(
                    host=self.host,
                    auto_reserve=True,
                )
                logger.info("RelayDiscovery created (auto_reserve=True, manual reservations)")
            except Exception as e:
                    logger.warning(f"RelayDiscovery init failed (non-fatal): {e}")

        # Dcutr protocol check 
        if self.enable_dcutr and self.relay_addrs:
            try:
                self.dcutr = DCUtRProtocol(host=self.host)
                logger.info("DCUtRProtocol created (hole punching enabled)")
            except Exception as e:
                logger.warning(f"DCUtRProtocol init failed (non-fatal): {e}")
        elif self.enable_dcutr and not self.relay_addrs:
            logger.info("ℹ️  DCUtR skipped — requires --relay to be set first")


        # Register identify protocol handler
        logger.info("📋 Registering identify protocol handler (raw protobuf format for go-libp2p compatibility)")
        identify_handler = identify_handler_for(self.host, use_varint_format=True)
        self.host.set_stream_handler(IDENTIFY_PROTOCOL_ID, identify_handler)
        logger.info(f"✅ Identify protocol handler registered for {IDENTIFY_PROTOCOL_ID} (raw format)")

        # Create DHT with random walk enabled
        self.dht = KadDHT(self.host, DHTMode.SERVER, enable_random_walk=True)
        logger.info("✅ DHT created with random walk enabled")
        
        self.full_multiaddr = f"{listen_addr}/p2p/{self.host.get_id()}"
        logger.info(f"Host created with PeerID: {self.host.get_id()}")
        logger.info(f"Listening on: {listen_addr}")
        logger.info(f"Full multiaddr: {self.full_multiaddr}")
        
        # Log GossipSub protocol configuration
        logger.info(f"📋 Configuring GossipSub with protocols: {PROTOCOL_ID_LIST}")
        logger.info(f"  Protocol 1: {PROTOCOL_ID}")
        logger.info(f"  Protocol 2: {PROTOCOL_ID_V11}")
        
        # Create GossipSub with optimized parameters (matching working pubsub.py)
        self.gossipsub = GossipSub(
            protocols=PROTOCOL_ID_LIST,
            degree=3,
            degree_low=2,
            degree_high=4,
            gossip_window=2,  # Smaller window for faster gossip
            gossip_history=5,  # Keep more history
            heartbeat_initial_delay=2.0,  # Start heartbeats sooner
            heartbeat_interval=5,  # More frequent heartbeats for testing
        )
        logger.info("✅ GossipSub router created successfully")
        
        # Create PubSub
        logger.info(f"🔐 Creating PubSub with strict_signing={self.strict_signing}")
        self.pubsub = Pubsub(self.host, self.gossipsub, strict_signing=self.strict_signing)
        logger.info("✅ PubSub service created successfully")
        
        # Start host and pubsub services
        async with self.host.run(listen_addrs=[listen_addr]):
            logger.info("📡 Initializing PubSub, GossipSub, and DHT services...")
            try:
                async with background_trio_service(self.pubsub):
                    async with background_trio_service(self.gossipsub):
                        async with background_trio_service(self.dht):
                            await self._start_nat_and_run()

            except (MultiselectClientError, StreamFailure) as e:
                logger.log(f"The protocol negotitaion failed: {e}")
                pass
    
    async def _start_nat_and_run(self):
        """
        Start NAT traversal services as background tasks, then run the main loop.
        Uses background_trio_service (original approach) to avoid triggering the
        remove_stream_handler cleanup path that nursery.start() exposes.
        Ready flag is set before relay setup so the UI never hangs.
        """
        logger.info("Pubsub, GossipSub, DHT started.")
        await self.pubsub.wait_until_ready()
        logger.info("Pubsub ready.")

        # DEBUG relay server state
        await self._send_system_message(
            f"relay_server_mode={self.relay_server_mode}, "
            f"circuit_v2={self.circuit_v2 is not None}, "
            f"allow_hop={getattr(self.circuit_v2, 'allow_hop', 'N/A')}"
        )
        logger.info("Pubsub ready.")

        async with trio.open_nursery() as nat_nursery:
            # Start background NAT services using background_trio_service —
            # same as original, avoids triggering remove_stream_handler on cleanup
            if self.circuit_v2 is not None:
                nat_nursery.start_soon(
                    self._run_background_service, self.circuit_v2, "CircuitV2Protocol"
                )
                await trio.sleep(1)
                # DEBUG - correct way to check registered handlers in py-libp2p
                try:
                    mux = self.host.get_mux()
                    # Try different ways to list protocols
                    if hasattr(mux, 'handlers'):
                        protos = list(mux.handlers.keys())
                    elif hasattr(mux, '_handlers'):
                        protos = list(mux._handlers.keys())
                    elif hasattr(mux, 'get_protocols'):
                        protos = mux.get_protocols()
                    else:
                        protos = [attr for attr in dir(mux)]  # last resort — dump attributes
                    
                    hop = "/libp2p/circuit/relay/0.2.0/hop"
                    stop = "/libp2p/circuit/relay/0.2.0/stop"
                    await self._send_system_message(f"Registered protocols: {protos}")
                    await self._send_system_message(
                        f"HOP: {hop in str(protos)}, STOP: {stop in str(protos)}"
                    )
                except Exception as e:
                    await self._send_system_message(f"Mux inspection failed: {e}")

            if self.relay_discovery is not None:
                nat_nursery.start_soon(
                    self._run_background_service, self.relay_discovery, "RelayDiscovery"
                )
                await trio.sleep(0.5)

            if self.dcutr is not None:
                nat_nursery.start_soon(
                    self._run_background_service, self.dcutr, "DCUtRProtocol"
                )
                await trio.sleep(0.5)

            # Extra delay to let services fully initialise their stream handlers
            # before we attempt any connections or reservations
            await trio.sleep(1)

            # Bootstrap discovery
            if BOOTSTRAP_PEERS:
                bootstrap = BootstrapDiscovery(self.host.get_network(), BOOTSTRAP_PEERS)
                await bootstrap.start()

            # Subscribe to topics before connecting
            await self._setup_chat_room()

            # Connect to regular peers
            await self._setup_connections()

            # Mark ready BEFORE relay setup — this unblocks run_headless_in_thread's
            # polling loop so the UI starts immediately. Relay negotiation is a
            # background concern and must never gate the ready flag.
            self.ready = True
            self.ready_event.set()
            logger.info("✅ Headless service is ready")
            await self._send_system_message("Service ready")

            # Relay setup runs after ready is set, with per-relay timeouts
            await self._setup_relay_connections()

            # Main processing loop
            async with trio.open_nursery() as main_nursery:
                main_nursery.start_soon(self._process_messages)
                main_nursery.start_soon(self._process_outgoing_messages)
                main_nursery.start_soon(self._process_topic_subscriptions)
                main_nursery.start_soon(self._process_peer_connections)
                main_nursery.start_soon(self._wait_for_stop)
                main_nursery.start_soon(self.monitor_peers)
                main_nursery.start_soon(maintain_connections, self.host)

    async def _run_background_service(self, service, name: str):
        """
        Generic wrapper to run trio-compatible background services.
        """
        try:
            logger.info(f"Starting {name} background service...")
            async with background_trio_service(service):
                await trio.sleep_forever()
        except Exception as e:
            logger.warning(f"{name} crashed: {e}")


    async def _setup_relay_connections(self):
        if not self.relay_addrs:
            return

        for addr_str in self.relay_addrs:
            logger.info(f"📡 Setting up relay connection: {addr_str}")
            try:
                addr = multiaddr.Multiaddr(addr_str)
                info = info_from_p2p_addr(addr)

                connected = False
                with trio.move_on_after(10) as connect_scope:
                    await self.host.connect(info)
                    connected = True
                    logger.info(f"✅ TCP connection established to relay: {info.peer_id}")

                if connect_scope.cancelled_caught:
                    logger.warning(
                        f"⚠️ Timed out connecting to relay {addr_str} after 10s — skipping"
                    )
                    await self._send_system_message(
                        f"Relay connection timed out: {addr_str}"
                    )
                    continue

                if not connected:
                    continue

                # Increased from 1s to 3s — gives the relay's CircuitV2Protocol
                # HOP stream handler enough time to fully initialise after our
                # TCP connection lands. Reservation denials happen when we send
                # the HOP RESERVE message before the relay is ready to handle it.
                await trio.sleep(3)

                try:
                    protos = self.host.get_peerstore().get_protocols(info.peer_id)
                    await self._send_system_message(f"Relay protocols: {protos}")
                    hop_proto = "/libp2p/circuit/relay/0.2.0/hop"
                    if hop_proto in protos:
                        await self._send_system_message("✅ Relay HAS hop protocol")
                    else:
                        await self._send_system_message("❌ Relay MISSING hop protocol — needs --relay-server")
                except Exception as e:
                    await self._send_system_message(f"Could not read relay protocols: {e}")

                await self._send_system_message(
                    f"Our circuit_v2: {self.circuit_v2 is not None}, "
                    f"allow_hop: {getattr(self.circuit_v2, 'allow_hop', 'N/A')}, "
                    f"relay_discovery: {self.relay_discovery is not None}"
                )


                if self.relay_discovery is not None:
                    # Retry loop — attempt reservation up to 3 times with backoff.
                    # Handles the race where relay is still initialising HOP handler.
                    reserved = False
                    for attempt in range(1, 4):
                        with trio.move_on_after(15) as reserve_scope:
                            reserved = await self.relay_discovery.make_reservation(
                                info.peer_id
                            )

                        if reserve_scope.cancelled_caught:
                            logger.warning(
                                f"⚠️ make_reservation timed out (attempt {attempt}/3) "
                                f"for relay {info.peer_id}"
                            )
                            await trio.sleep(2 * attempt)  # backoff: 2s, 4s, 6s
                            continue

                        if reserved:
                            logger.info(
                                f"✅ Relay reservation granted by {info.peer_id} "
                                f"(attempt {attempt}/3)"
                            )
                            await self._send_system_message(
                                f"Relay active via {str(info.peer_id)[:12]}"
                            )
                            break
                        else:
                            logger.warning(
                                f"❌ Reservation denied by {info.peer_id} "
                                f"(attempt {attempt}/3) — retrying in {2 * attempt}s"
                            )
                            await self._send_system_message(
                                f"Relay reservation denied (attempt {attempt}/3), retrying..."
                            )
                            await trio.sleep(2 * attempt)  # backoff: 2s, 4s, 6s

                    if not reserved:
                        logger.warning(
                            f"❌ All 3 reservation attempts failed for {info.peer_id} — "
                            f"relay may not be in HOP server mode or is overloaded"
                        )
                        await self._send_system_message(
                            f"Relay reservation failed after 3 attempts: "
                            f"{str(info.peer_id)[:12]}"
                        )
                else:
                    logger.info(
                        f"ℹ️ Connected to relay {info.peer_id} but no RelayDiscovery — "
                        f"skipping reservation"
                    )

            except Exception as e:
                logger.warning(
                    f"⚠️ Relay setup failed for {addr_str} (non-fatal): {e}"
                )
                await self._send_system_message(
                    f"Relay setup failed: {addr_str}"
                )


    async def _request_relay_reservation(self, relay_peer_id: ID):
        """
        Request a Circuit Relay v2 reservation via RelayDiscovery.make_reservation().
        Returns True on success (STATUS_OK from relay), False if denied.
        Failure is non-fatal — direct connections still work without a reservation.
        """
        if self.relay_discovery is None:
            logger.info(f"ℹ️  No RelayDiscovery — skipping reservation for {relay_peer_id}")
            return

        try:
            logger.info(f"📡 Requesting relay reservation from: {relay_peer_id}")
            # RelayDiscovery.make_reservation(peer_id: ID) -> bool
            success = await self.relay_discovery.make_reservation(relay_peer_id)
            if success:
                logger.info(f"✅ Relay reservation granted by {relay_peer_id}")
                await self._send_system_message(
                    f"Relay reservation active via {str(relay_peer_id)[:12]}"
                )
            else:
                logger.warning(f"⚠️  Relay {relay_peer_id} denied reservation")
                await self._send_system_message(
                    f"Relay reservation denied by {str(relay_peer_id)[:12]}"
                )
        except Exception as e:
            logger.warning(f"⚠️  Relay reservation failed (non-fatal): {e}")

    async def _setup_connections(self):
        """Setup connections to specified peers with detailed protocol logging."""
        if not self.connect_addrs:
            return
        
        for addr_str in self.connect_addrs:
            try:
                logger.info(f"🔗 Attempting to connect to: {addr_str}")
                maddr = multiaddr.Multiaddr(addr_str)
                info = info_from_p2p_addr(maddr)
                logger.info(f"🔗 Parsed peer info - ID: {info.peer_id}, Addrs: {info.addrs}")
                
                # Check if already connected
                existing_conns = self.host.get_network().connections.get(info.peer_id)
                if existing_conns:
                    logger.info(f"✅ Already connected to peer: {info.peer_id}, skipping connection attempt")
                    continue
                
                # Log connection attempt
                logger.info(f"🔗 Initiating connection to peer: {info.peer_id}")
                await self.host.connect(info)
                logger.info(f"✅ TCP connection established to peer: {info.peer_id}")
                
                # Wait longer for protocol negotiation
                await trio.sleep(3)
                
                # Detailed protocol inspection
                logger.info(f"🔍 Starting protocol inspection for peer: {info.peer_id}")
                await self._inspect_peer_protocols(info.peer_id)
                
                # Check connection status
                try:
                    # In py-libp2p, we can check if peer is connected via the swarm
                    swarm = self.host.get_network()
                    if hasattr(swarm, 'connections') and info.peer_id in swarm.connections:
                        connections = [swarm.connections[info.peer_id]]
                        logger.info(f"📊 Active connections to peer {info.peer_id}: {len(connections)}")
                    else:
                        logger.info(f"📊 No direct connection info available for peer {info.peer_id}")
                except Exception as conn_err:
                    logger.warning(f"⚠️  Could not check connection status: {conn_err}")
                
                # Wait for PubSub protocol negotiation
                logger.info(f"⏳ Waiting for PubSub protocol negotiation...")
                await trio.sleep(3)
                
                # Check final PubSub status
                await self._check_pubsub_status(info.peer_id)
                
                await self._send_system_message(f"Connected to peer: {str(info.peer_id)[:8]}")
                
            except Exception as e:
                logger.error(f"❌ Failed to connect to {addr_str}: {e}")
                await self._send_system_message(f"Failed to connect to {addr_str}: {e}")
    
    async def _inspect_peer_protocols(self, peer_id):
        """Inspect and log all protocols supported by a peer."""
        try:
            logger.info(f"🔍 Checking peerstore for peer: {peer_id}")
            
            # Get peer's protocols from peerstore (simplified approach)
            peerstore = self.host.get_peerstore()
            
            # Check if we can access protocols - different py-libp2p versions have different APIs
            try:
                if hasattr(peerstore, 'get_protocols'):
                    protocols = peerstore.get_protocols(peer_id)
                elif hasattr(peerstore, 'protocols'):
                    protocols = peerstore.protocols(peer_id)
                else:
                    # Fallback - just log that we connected successfully
                    logger.info(f"✅ Successfully connected to peer {peer_id}")
                    logger.info(f"🔍 Protocol inspection not available in this py-libp2p version")
                    return
                    
                if protocols:
                    logger.info(f"📋 Peer {peer_id} supports {len(protocols)} protocols:")
                    for i, protocol in enumerate(protocols, 1):
                        logger.info(f"  {i}: {protocol}")
                        if "meshsub" in str(protocol) or "gossipsub" in str(protocol):
                            logger.info(f"  🎯 Found PubSub protocol: {protocol}")
                else:
                    logger.info(f"📋 No protocols found for peer {peer_id} yet (may still be negotiating)")
                    
            except Exception as proto_err:
                logger.info(f"🔍 Protocol details not accessible: {proto_err}")
                logger.info(f"✅ Peer {peer_id} connected successfully")
                    
        except Exception as e:
            logger.warning(f"⚠️  Error inspecting peer protocols: {e}")
            logger.info(f"✅ Peer {peer_id} connected successfully")
    
    async def _check_pubsub_status(self, peer_id):
        """Check the PubSub connection status with a specific peer."""
        try:
            logger.info(f"🔍 Checking PubSub status for peer: {peer_id}")
            
            # Check if peer is in pubsub.peers
            pubsub_peers = list(self.pubsub.peers.keys())
            logger.info(f"📡 Total PubSub peers: {len(pubsub_peers)}")
            for i, p in enumerate(pubsub_peers, 1):
                logger.info(f"  PubSub peer {i}: {p}")
            
            if peer_id in self.pubsub.peers:
                logger.info(f"✅ Peer {peer_id} is in PubSub mesh")
                
                # Check GossipSub specific status
                if hasattr(self.pubsub, 'router') and hasattr(self.pubsub.router, 'mesh'):
                    mesh = self.pubsub.router.mesh
                    logger.info(f"🕸️  GossipSub mesh status:")
                    logger.info(f"    Mesh topics: {list(mesh.keys())}")
                    for topic, topic_peers in mesh.items():
                        logger.info(f"    Topic '{topic}': {len(topic_peers)} peers")
                        if peer_id in topic_peers:
                            logger.info(f"    ✅ Peer {peer_id} is in mesh for topic '{topic}'")
                        else:
                            logger.warning(f"    ❌ Peer {peer_id} is NOT in mesh for topic '{topic}'")
            else:
                logger.warning(f"❌ Peer {peer_id} is NOT in PubSub mesh")
                logger.info("🔧 Possible reasons:")
                logger.info("  1. PubSub protocol negotiation failed")
                logger.info("  2. Peer doesn't support compatible GossipSub version")
                logger.info("  3. Network issues preventing PubSub handshake")
                
        except Exception as e:
            logger.error(f"❌ Error checking PubSub status: {e}")
    
    async def _setup_chat_room(self):
        """Setup the chat room."""
        logger.info("Setting up chat room...")
        
        self.chat_room = await ChatRoom.join_chat_room(
            host=self.host,
            pubsub=self.pubsub,
            nickname=self.nickname,
            multiaddr=self.full_multiaddr,
            headless_service=self,
            topic=self.topic
        )
        
        # Add custom message handler to forward messages to UI
        self.chat_room.add_message_handler(self._handle_chat_message)
        
        # Start message handlers
        self.running = True
        
        logger.info(f"Chat room setup complete for '{self.nickname}'")
        await self._send_system_message(f"Joined chat room as '{self.nickname}'")
    
    async def _handle_chat_message(self, message: ChatMessage):
        """Handle incoming chat messages and store them per-topic."""
        try:
            topic = message.topic or "default"
            
            # Initialize topic storage if needed
            if topic not in self.topic_messages:
                self.topic_messages[topic] = []
                self.topic_unread_counts[topic] = 0
            
            # Store message with unread flag
            message_data = {
                'type': 'chat_message',
                'message': message.message,
                'sender_nick': message.sender_nick,
                'sender_id': message.sender_id,
                'timestamp': message.timestamp,
                'topic': topic,
                'read': False  # New messages are unread by default
            }
            
            self.topic_messages[topic].append(message_data)
            self.topic_unread_counts[topic] += 1
            
            # Log in simplified format only if not in UI mode
            if not self.ui_mode:
                logger.info(f"[{topic}] {message.sender_nick}: {message.message}")
            
            # Still put message in queue for UI updates
            await self.message_queue.async_q.put(message_data)
            
        except Exception as e:
            logger.error(f"Error handling chat message: {e}")
            logger.exception("Full traceback:")
    
    async def _send_system_message(self, message: str):
        """Send system message to UI queue."""
        logger.debug(f"_send_system_message called with: {message}")
        try:
            if self.system_queue:
                logger.debug(f"System queue available, sending message: {message}")
                await self.system_queue.async_q.put({
                    'type': 'system_message',
                    'message': message,
                    'timestamp': trio.current_time()
                })
                logger.debug(f"System message sent successfully: {message}")
            else:
                logger.warning(f"System queue not available, cannot send message: {message}")
        except Exception as e:
            logger.error(f"Error sending system message: {e}")
            logger.exception("Full traceback:")
    
    async def _process_messages(self):
        """Process messages from chat room."""
        try:
            # Start chat room message handlers
            await self.chat_room.start_message_handlers()
        except Exception as e:
            logger.error(f"Error in message processing: {e}")
    
    async def _process_outgoing_messages(self):
        """Process outgoing messages from UI to chat room."""
        
        while self.running:
            try:
                # Check for messages from UI (non-blocking)
                try:
                    outgoing_data = self.outgoing_queue.sync_q.get_nowait()
                    if outgoing_data and 'message' in outgoing_data:
                        message = outgoing_data['message']
                        topic = outgoing_data.get('topic')  # Optional topic parameter
                        
                        # Send message through chat room
                        if self.chat_room and self.running:
                            if topic:
                                # Send to specific topic
                                success = await self.chat_room.publish_to_topic(topic, message)
                                if not self.ui_mode:
                                    logger.info(f"{self.nickname} (you) to {topic}: {message}")
                            else:
                                # Send to default chat topic
                                await self.chat_room.publish_message(message)
                                if not self.ui_mode:
                                    logger.info(f"{self.nickname} (you): {message}")
                        else:
                            logger.warning("Cannot send message: chat room not ready")
                            await self._send_system_message("Cannot send message: chat room not ready")
                            
                except Empty:
                    # No message available, that's fine
                    await trio.sleep(0.1)  # Brief pause to avoid busy loop
                except Exception as e:
                    logger.error(f"Error processing outgoing message: {e}")
                    await trio.sleep(0.1)
                    
            except Exception as e:
                logger.error(f"Error in outgoing message processing: {e}")
                await trio.sleep(0.1)
    
    async def _process_topic_subscriptions(self):
        """Process topic subscription requests from UI."""
        
        while self.running:
            try:
                # Check for subscription requests from UI (non-blocking)
                try:
                    subscription_data = self.topic_subscription_queue.sync_q.get_nowait()
                    if subscription_data and 'topic' in subscription_data:
                        topic_name = subscription_data['topic']
                        
                        # Subscribe to the topic through chat room
                        if self.chat_room and self.running:
                            success = await self.chat_room.subscribe_to_topic(topic_name)
                            if success:
                                logger.info(f"Successfully subscribed to topic: {topic_name}")
                                await self._send_system_message(f"Subscribed to topic: {topic_name}")
                            else:
                                logger.warning(f"Failed to subscribe to topic: {topic_name}")
                                await self._send_system_message(f"Failed to subscribe to topic: {topic_name}")
                        else:
                            logger.warning("Cannot subscribe to topic: chat room not ready")
                            await self._send_system_message("Cannot subscribe to topic: chat room not ready")
                            
                except Empty:
                    # No request available, that's fine
                    await trio.sleep(0.1)  # Brief pause to avoid busy loop
                except Exception as e:
                    logger.error(f"Error processing topic subscription: {e}")
                    await trio.sleep(0.1)
                    
            except Exception as e:
                logger.error(f"Error in topic subscription processing: {e}")
                await trio.sleep(0.1)
    
    async def _process_peer_connections(self):
        """Process peer connection requests from UI."""
        
        while self.running:
            try:
                # Check for connection requests from UI (non-blocking)
                try:
                    multiaddr_str = self.peer_connection_queue.sync_q.get_nowait()
                    if multiaddr_str:
                        logger.info(f"Processing peer connection request: {multiaddr_str}")
                        
                        # Parse and connect to the peer
                        try:
                            # Parse the multiaddress
                            maddr = multiaddr.Multiaddr(multiaddr_str)
                            
                            # Try to get peer info from the multiaddress
                            peer_info = info_from_p2p_addr(maddr)
                            
                            if peer_info:
                                # Connect to the peer
                                logger.info(f"Attempting to connect to peer: {peer_info.peer_id}")
                                await self.host.connect(peer_info)
                                logger.info(f"✅ Successfully connected to peer: {peer_info.peer_id}")
                                await self._send_system_message(f"Connected to peer: {peer_info.peer_id}")
                            else:
                                logger.error(f"Could not extract peer info from multiaddress: {multiaddr_str}")
                                await self._send_system_message(f"Invalid multiaddress format")
                                
                        except Exception as e:
                            logger.error(f"Failed to connect to peer {multiaddr_str}: {e}")
                            await self._send_system_message(f"Connection failed: {str(e)}")
                            
                except Empty:
                    # No request available, that's fine
                    await trio.sleep(0.1)  # Brief pause to avoid busy loop
                except Exception as e:
                    logger.error(f"Error processing peer connection: {e}")
                    await trio.sleep(0.1)
                    
            except Exception as e:
                logger.error(f"Error in peer connection processing: {e}")
                await trio.sleep(0.1)

    async def _wait_for_stop(self):
        """Wait for stop signal."""
        await self.stop_event.wait()
        logger.info("Stop signal received, shutting down...")
        self.running = False
    
    def send_message(self, message: str):
        """Send a message through the chat room (thread-safe)."""
        if self.outgoing_queue and self.running:
            try:
                # Put message in outgoing queue (sync call, safe from UI thread)
                self.outgoing_queue.sync_q.put({
                    'message': message,
                    'timestamp': time.time()
                })
            except Exception as e:
                logger.error(f"Failed to queue message: {e}")
        else:
            logger.warning("Cannot send message: outgoing queue not ready or service not running")
    
    def send_message_to_topic(self, topic: str, message: str):
        """Send a message to a specific topic (thread-safe)."""
        if self.outgoing_queue and self.running:
            try:
                # Put message with topic in outgoing queue
                self.outgoing_queue.sync_q.put({
                    'message': message,
                    'topic': topic,
                    'timestamp': time.time()
                })
            except Exception as e:
                logger.error(f"Failed to queue message to topic {topic}: {e}")
        else:
            logger.warning("Cannot send message: outgoing queue not ready or service not running")
    
    def get_connection_info(self) -> Dict[str, Any]:
        """Get connection information for UI."""
        if not self.ready:
            return {}

        all_addrs = self.host.get_addrs()
        relay_paths = [str(a) for a in all_addrs if "p2p-circuit" in str(a)]

        return {
            'peer_id': str(self.host.get_id()),
            'nickname': self.nickname,
            'multiaddr': self.full_multiaddr, # Your direct address
            'relay_addrs': relay_paths,       # YOUR NEW RELAYED ADDRESSES
            'connected_peers': self.chat_room.get_connected_peers() if self.chat_room else set(),
            'peer_count': self.chat_room.get_peer_count() if self.chat_room else 0,
            'autonat_status': self.autonat.get_status() if self.autonat else 0
        }
        
    def get_subscribed_topics(self) -> Set[str]:
        """Get list of all subscribed topics."""
        if not self.chat_room:
            return set()
        return self.chat_room.get_subscribed_topics()
    
    def subscribe_to_topic(self, topic_name: str) -> bool:
        """
        Subscribe to a new topic (thread-safe wrapper).
        
        Args:
            topic_name: The name of the topic to subscribe to
            
        Returns:
            True if subscription request was queued, False otherwise
        """
        if not self.chat_room or not self.running:
            logger.warning("Cannot subscribe to topic: chat room not ready or service not running")
            return False
        
        try:
            # Put subscription request in queue (sync call, safe from UI thread)
            self.topic_subscription_queue.sync_q.put({
                'topic': topic_name,
                'timestamp': time.time()
            })
            logger.info(f"Queued subscription request for topic: {topic_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to queue topic subscription: {e}")
            return False
    
    def connect_to_peer(self, multiaddr: str) -> bool:
        """
        Connect to a peer using multiaddress (thread-safe wrapper).
        
        Args:
            multiaddr: The multiaddress of the peer to connect to
            
        Returns:
            True if connection request was queued, False otherwise
        """
        if not self.host or not self.running:
            logger.warning("Cannot connect to peer: host not ready or service not running")
            return False
        
        try:
            # Put connection request in queue (sync call, safe from UI thread)
            self.peer_connection_queue.sync_q.put(multiaddr)
            logger.info(f"Queued peer connection request: {multiaddr}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to queue peer connection: {e}")
            return False
    
    def get_message_queue(self):
        """Get the message queue for UI."""
        return self.message_queue
    
    def get_system_queue(self):
        """Get the system queue for UI."""
        return self.system_queue
    
    def get_topic_messages(self, topic: str) -> List[Dict[str, Any]]:
        """
        Get all messages for a specific topic.
        
        Args:
            topic: The topic name
            
        Returns:
            List of message dictionaries
        """
        return self.topic_messages.get(topic, [])
    
    def get_all_topics_with_info(self) -> Dict[str, Dict[str, Any]]:
        """
        Get all subscribed topics with their message counts and unread status.
        
        Returns:
            Dict mapping topic names to info dicts containing:
            - unread_count: Number of unread messages
            - total_count: Total number of messages
            - last_message: Most recent message (if any)
        """
        result = {}
        subscribed_topics = self.get_subscribed_topics()
        
        for topic in subscribed_topics:
            messages = self.topic_messages.get(topic, [])
            unread_count = self.topic_unread_counts.get(topic, 0)
            
            info = {
                'unread_count': unread_count,
                'total_count': len(messages),
                'last_message': messages[-1] if messages else None
            }
            result[topic] = info
        
        return result
    
    def mark_topic_as_read(self, topic: str):
        """
        Mark all messages in a topic as read.
        
        Args:
            topic: The topic name
        """
        if topic in self.topic_messages:
            for message in self.topic_messages[topic]:
                message['read'] = True
            self.topic_unread_counts[topic] = 0
            logger.debug(f"Marked all messages in topic '{topic}' as read")
    
    def get_unread_count(self, topic: str) -> int:
        """
        Get the count of unread messages for a topic.
        
        Args:
            topic: The topic name
            
        Returns:
            Number of unread messages
        """
        return self.topic_unread_counts.get(topic, 0)
    
    def get_outgoing_queue(self):
        """Get the outgoing queue for UI to send messages."""
        return self.outgoing_queue
    
    async def get_peer_info_via_identify(self, peer_id):
        """Get peer information using official identify protocol implementation."""
        try:
            logger.info(f"🔍 Requesting identify info from peer: {peer_id}")
            logger.info(f"peers in peer store are: {self.host.get_peerstore().peers_with_addrs()}")
            logger.info(f"address of peer {peer_id} is {self.host.get_peerstore().peer_info(peer_id).addrs} ")
            
            # Create a stream to the peer for identify protocol - use tuple format as in example
            stream = await self.host.new_stream(peer_id, (IDENTIFY_PROTOCOL_ID,))
            
            try:
                # Use official py-libp2p utilities to read the response
                # Use raw protobuf format (use_varint_format=False) for go-libp2p compatibility
                # go-libp2p uses the old/raw format, not the newer varint length-prefixed format
                response_bytes = await read_length_prefixed_protobuf(stream, use_varint_format=True)
                
                if not response_bytes:
                    logger.warning(f"Empty identify response from peer: {peer_id}")
                    return None
                
                # Parse the identify response using official parser
                identify_info = parse_identify_response(response_bytes)
                
                logger.info(f"✅ Received identify info from {peer_id}")
                logger.info(f"  - Protocol Version: {identify_info.protocol_version}")
                logger.info(f"  - Agent Version: {identify_info.agent_version}")
                logger.info(f"  - Public Key: {len(identify_info.public_key)} bytes")
                logger.info(f"  - Listen Addresses: {len(identify_info.listen_addrs)} addresses")
                logger.info(f"  - Protocols: {len(identify_info.protocols)} protocols")
                
                # Store the peer info in our cache
                self.peer_info_cache[str(peer_id)] = {
                    'public_key': identify_info.public_key,
                    'protocol_version': identify_info.protocol_version,
                    'agent_version': identify_info.agent_version,
                    'listen_addrs': identify_info.listen_addrs,
                    'protocols': identify_info.protocols,
                    'timestamp': time.time()
                }
                
                return identify_info
                
            finally:
                await stream.close()
                
        except Exception as e:
            logger.error(f"❌ Failed to get identify info from peer {peer_id}: {e}")
            return None
    
    async def get_cached_peer_info(self, peer_id: str):
        """Get cached peer info, or fetch it if not available."""
        peer_id_str = str(peer_id)
        
        # Check if we have cached info
        if peer_id_str in self.peer_info_cache:
            cached_info = self.peer_info_cache[peer_id_str]
            # Check if cache is not too old (5 minutes)
            if time.time() - cached_info['timestamp'] < 300:
                return cached_info
            else:
                logger.debug(f"Cached info for {peer_id_str} is stale, refreshing")
        
        # Fetch fresh info
        try:
            peer_id_obj = ID.from_base58(peer_id_str) if isinstance(peer_id, str) else peer_id
            identify_info = await self.get_peer_info_via_identify(peer_id_obj)
            
            if identify_info:
                return self.peer_info_cache[peer_id_str]
        except Exception as e:
            logger.error(f"❌ Failed to get peer info for {peer_id_str}: {e}")
        
        return None
    
    def get_public_key_for_peer(self, peer_id: str):
        """Get public key for a peer (synchronous access to cache)."""
        peer_id_str = str(peer_id)
        if peer_id_str in self.peer_info_cache:
            return self.peer_info_cache[peer_id_str]['public_key']
        return None
    
    async def stop(self):
        """Stop the headless service."""
        logger.info("Stopping headless service...")
        self.stop_event.set()
        
        if self.chat_room:
            await self.chat_room.stop()
        
        # Close queues
        if self.message_queue:
            self.message_queue.close()
        if self.system_queue:
            self.system_queue.close()
        if self.outgoing_queue:
            self.outgoing_queue.close()
        if self.topic_subscription_queue:
            self.topic_subscription_queue.close()
        if self.peer_connection_queue:
            self.peer_connection_queue.close()
        
        logger.info("Headless service stopped")
