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
from libp2p.relay.circuit_v2.protocol import (
    CircuitV2Protocol,
    PROTOCOL_ID as RELAY_PROTOCOL_ID,
    STOP_PROTOCOL_ID as RELAY_STOP_PROTOCOL_ID,
)
from libp2p.relay.circuit_v2.config import RelayConfig, RelayRole
from libp2p.relay.circuit_v2.resources import RelayLimits
from libp2p.relay.circuit_v2.discovery import RelayDiscovery, RelayInfo 
from libp2p.relay.circuit_v2.transport import CircuitV2Transport
from libp2p.relay.circuit_v2.dcutr import DCUtRProtocol
                               

from chatroom.chatroom import ChatRoom, ChatMessage
from utils.constants import * 
logger = logging.getLogger("headless")

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
        self.relay_config: RelayConfig = None        # RelayConfig instance
        self.circuit_v2_transport: CircuitV2Transport = None  # CircuitV2Transport instance
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

        # Configure relay if enabled (but don't register handlers yet - wait for host.run())
        if self.relay_server_mode or self.relay_addrs:
            try:
                # Configure relay limits (following relay_example.py)
                limits = RelayLimits(
                    duration=DEFAULT_RELAY_LIMIT_DURATION,  
                    data=DEFAULT_RELAY_LIMIT_DATA_SIZE, 
                    max_circuit_conns=DEFAULT_RELAY_MAX_CIRCUIT_CONNS,
                    max_reservations=DEFAULT_RELAY_MAX_RESERVATIONS,
                )
                
                # Configure relay roles
                if self.relay_server_mode:
                    # Relay server: HOP + STOP + CLIENT
                    roles = RelayRole.HOP | RelayRole.STOP | RelayRole.CLIENT
                else:
                    # Relay client: STOP + CLIENT only
                    roles = RelayRole.STOP | RelayRole.CLIENT
                
                self.relay_config = RelayConfig(
                    roles=roles,
                    limits=limits,
                )
                
                # Create CircuitV2Protocol with limits
                self.circuit_v2 = CircuitV2Protocol(
                    host=self.host,
                    limits=limits,
                    allow_hop=self.relay_server_mode,
                )
                mode_label = "HOP server" if self.relay_server_mode else "STOP client"
                logger.info(f"CircuitV2Protocol created ({mode_label} mode) with limits")
                logger.info(f"  - Relay roles: {roles}")
                logger.info(f"  - Duration: {limits.duration}s, Data: {limits.data} bytes")
                logger.info(f"  - Max connections: {limits.max_circuit_conns}, Max reservations: {limits.max_reservations}")
            except Exception as e:
                logger.warning(f"CircuitV2Protocol init failed (non-fatal): {e}")

        # Dcutr protocol check 
        if self.enable_dcutr and self.relay_addrs:
            try:
                self.dcutr = DCUtRProtocol(host=self.host)
                logger.info("DCUtRProtocol created (hole punching enabled)")
            except Exception as e:
                logger.warning(f"DCUtRProtocol init failed (non-fatal): {e}")
        elif self.enable_dcutr and not self.relay_addrs:
            logger.info(" DCUtR skipped — requires --relay to be set first")


        # Register identify protocol handler
        logger.info("Registering identify protocol handler (raw protobuf format for go-libp2p compatibility)")
        identify_handler = identify_handler_for(self.host, use_varint_format=True)
        self.host.set_stream_handler(IDENTIFY_PROTOCOL_ID, identify_handler)
        logger.info(f"Identify protocol handler registered for {IDENTIFY_PROTOCOL_ID} (raw format)")

        # Create DHT with random walk enabled
        self.dht = KadDHT(self.host, DHTMode.SERVER, enable_random_walk=True)
        logger.info("DHT created with random walk enabled")
        
        self.full_multiaddr = f"{listen_addr}/p2p/{self.host.get_id()}"
        logger.info(f"Host created with PeerID: {self.host.get_id()}")
        logger.info(f"Listening on: {listen_addr}")
        logger.info(f"Full multiaddr: {self.full_multiaddr}")
        
        # Log GossipSub protocol configuration
        logger.info(f"Configuring GossipSub with protocols: {PROTOCOL_ID_LIST}")
        logger.info(f"Protocol 1: {PROTOCOL_ID}")
        logger.info(f"Protocol 2: {PROTOCOL_ID_V11}")
        
        # Create GossipSub with optimized parameters (matching working pubsub.py)
        self.gossipsub = GossipSub(
            protocols=PROTOCOL_ID_LIST,
            degree=DEFAULT_GOSSIPSUB_DEGREE,
            degree_low=DEFAULT_GOSSIPSUB_DEGREE_LOW,
            degree_high=DEFAULT_GOSSIPSUB_DEGREE_HIGH,
            gossip_window=DEFAULT_GOSSIPSUB_GOSSIP_WINDOW, 
            gossip_history=DEFAULT_GOSSIPSUB_GOSSIP_HISTORY,
            heartbeat_initial_delay=DEFAULT_GOSSIPSUB_HEARTBEAT_INITIAL_DELAY, 
            heartbeat_interval=DEFAULT_GOSSIPSUB_HEARTBEAT_INTERVAL, 
        )
        logger.info(" GossipSub router created successfully")
        
        # Create PubSub
        logger.info(f"Creating PubSub with strict_signing={self.strict_signing}")
        self.pubsub = Pubsub(self.host, self.gossipsub, strict_signing=self.strict_signing)
        logger.info("PubSub service created successfully")
        
        # Start host and pubsub services
        async with self.host.run(listen_addrs=[listen_addr]):
            logger.info("📡 Initializing PubSub, GossipSub, and DHT services...")
            try:
                async with background_trio_service(self.pubsub):
                    async with background_trio_service(self.gossipsub):
                        async with background_trio_service(self.dht):
                            await self._start_nat_and_run()

            except (MultiselectClientError, StreamFailure) as e:
                logger.error(f"The protocol negotiation failed: {e}")
                await self._send_system_message(f"Protocol negotiation failed: {e}")
    
    async def _start_nat_and_run(self):
        """
        Start NAT traversal services as background tasks, then run the main loop.
        Stream handlers are registered AFTER host.run() starts to ensure proper protocol advertisement.
        """
        logger.info("Pubsub, GossipSub, DHT started.")
        await self.pubsub.wait_until_ready()
        logger.info("Pubsub ready.")

        # Register circuit relay stream handlers AFTER host.run() starts 
        if self.circuit_v2 is not None:
            logger.info("[Relay] Registering circuit relay stream handlers...")
            self.host.set_stream_handler(RELAY_PROTOCOL_ID, self.circuit_v2._handle_hop_stream)
            self.host.set_stream_handler(RELAY_STOP_PROTOCOL_ID, self.circuit_v2._handle_stop_stream)
            logger.info(f"[Relay] Stream handlers registered: {RELAY_PROTOCOL_ID}, {RELAY_STOP_PROTOCOL_ID}")
            await self._send_system_message(
                f"[Relay] Handlers registered - HOP: {RELAY_PROTOCOL_ID}, STOP: {RELAY_STOP_PROTOCOL_ID}"
            )
            
            # Create CircuitV2Transport (REQUIRED for relay to work)
            self.circuit_v2_transport = CircuitV2Transport(self.host, self.circuit_v2, self.relay_config)
            logger.info("[Relay] CircuitV2Transport created")
            logger.info(
                f"[Relay] Transport config - enable_hop={self.relay_config.enable_hop}, "
                f"enable_stop={self.relay_config.enable_stop}, enable_client={self.relay_config.enable_client}"
            )
            await self._send_system_message(
                f"[Relay] Transport initialized - HOP: {self.relay_config.enable_hop}, "
                f"STOP: {self.relay_config.enable_stop}, CLIENT: {self.relay_config.enable_client}"
            )
            
            # If we're a client with relay addresses, create and link discovery service
            if self.relay_addrs and not self.relay_server_mode:
                try:
                    # Create discovery with auto_reserve=True for automatic relay reservations
                    self.relay_discovery = RelayDiscovery(
                        host=self.host,
                        auto_reserve=True,  # Enable automatic reservations
                    )
                    # Link discovery to transport
                    self.circuit_v2_transport.discovery = self.relay_discovery
                    logger.info("[Relay] RelayDiscovery created and linked to transport (auto_reserve=True)")
                    await self._send_system_message("[Relay] Discovery service initialized for automatic reservations")
                except Exception as e:
                    logger.warning(f"RelayDiscovery creation failed: {e}")

        # DEBUG relay server state
        await self._send_system_message(
            f"[Debug] relay_server_mode={self.relay_server_mode}, "
            f"circuit_v2={self.circuit_v2 is not None}, "
            f"allow_hop={getattr(self.circuit_v2, 'allow_hop', 'N/A')}"
        )

        async with trio.open_nursery() as nat_nursery:
            # Start CircuitV2Protocol as background service
            if self.circuit_v2 is not None:
                nat_nursery.start_soon(
                    self._run_background_service, self.circuit_v2, "CircuitV2Protocol"
                )
                await trio.sleep(1)
                
            if self.relay_discovery is not None:
                nat_nursery.start_soon(
                    self._run_background_service, self.relay_discovery, "RelayDiscovery"
                )
                logger.info("[Relay] RelayDiscovery background service started")
                await self._send_system_message("[Relay] Discovery service running")
                await trio.sleep(1)
                
            if self.circuit_v2 is not None:
                try:
                    mux = self.host.get_mux()
                    if hasattr(mux, 'handlers'):
                        protos = list(mux.handlers.keys())
                    elif hasattr(mux, '_handlers'):
                        protos = list(mux._handlers.keys())
                    elif hasattr(mux, 'get_protocols'):
                        protos = mux.get_protocols()
                    else:
                        protos = [attr for attr in dir(mux)]
                    
                    hop_str = str(RELAY_PROTOCOL_ID) 
                    stop_str = str(RELAY_STOP_PROTOCOL_ID) 
                    await self._send_system_message(f"Registered protocols: {protos}")
                    await self._send_system_message(
                        f"HOP ({hop_str}): {hop_str in str(protos)}, "
                        f"STOP ({stop_str}): {stop_str in str(protos)}"
                    )
                except Exception as e:
                    await self._send_system_message(f"Mux inspection failed: {e}")

            if self.dcutr is not None:
                nat_nursery.start_soon(
                    self._run_background_service, self.dcutr, "DCUtRProtocol"
                )
                await trio.sleep(0.5)

            # Extra delay to let services fully initialise their stream handlers
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
            # polling loop so the UI starts immediately.
            self.ready = True
            self.ready_event.set()
            logger.info("Headless service is ready")
            await self._send_system_message("Service ready")

            # Relay setup runs after ready is set, with per-relay timeouts
            await self._setup_relay_connections()

            # Main processing loop
            async with trio.open_nursery() as main_nursery:
                main_nursery.start_soon(self._process_messages)
                main_nursery.start_soon(self._process_outgoing_messages)
                main_nursery.start_soon(self._process_topic_subscriptions)
                main_nursery.start_soon(self._process_peer_connections)
                main_nursery.start_soon(self._process_relay_connections)
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
        """Connect to relay servers and make reservations."""
        if not self.relay_addrs:
            return

        await self._send_system_message(f"[Relay] Starting relay setup with {len(self.relay_addrs)} relay(s)...")
        
        # If not running as relay server, wait longer for relay to fully start up and advertise protocols
        if not self.relay_server_mode:
            await self._send_system_message("[Relay] Waiting 5 seconds for relay server to initialize and advertise protocols...")
            await trio.sleep(5)

        for addr_str in self.relay_addrs:
            await self._send_system_message(f"[Relay] Processing relay: {addr_str}")
            try:
                await self._send_system_message(f"[Relay] Parsing multiaddr...")
                addr = multiaddr.Multiaddr(addr_str)
                info = info_from_p2p_addr(addr)
                await self._send_system_message(f"[Relay] Parsed relay peer ID: {info.peer_id}")
                await self._send_system_message(f"[Relay] Relay addresses: {info.addrs}")

                # Check if already connected
                if info.peer_id in self.host.get_network().connections:
                    await self._send_system_message(f"[Relay] Already connected to {info.peer_id}")
                    connected = True
                else:
                    await self._send_system_message(f"[Relay] Connecting to relay {info.peer_id}...")
                    connected = False
                    with trio.move_on_after(10) as connect_scope:
                        await self.host.connect(info)
                        connected = True
                        await self._send_system_message(f"[Relay] TCP connection established to relay: {info.peer_id}")

                    if connect_scope.cancelled_caught:
                        await self._send_system_message(f"[Relay] ⏱Timed out connecting to relay {addr_str} after 10s — skipping")
                        continue

                if not connected:
                    await self._send_system_message(f"[Relay] Failed to connect to relay")
                    continue

                # Give relay time to register stream handlers and advertise protocols
                await self._send_system_message(f"[Relay] Waiting 5s for relay protocol advertisement...")
                await trio.sleep(5)

                hop_proto = str(RELAY_PROTOCOL_ID)
                try:
                    protos = self.host.get_peerstore().get_protocols(info.peer_id)
                    proto_strs = [str(p) for p in protos] if protos else []
                    await self._send_system_message(f"[Relay] Relay protocols: {proto_strs}")
                    if hop_proto in proto_strs:
                        await self._send_system_message(f"[Relay] Relay HAS {hop_proto}")
                    else:
                        await self._send_system_message(f"[Relay] Relay MISSING {hop_proto}")
                        await self._send_system_message("[Relay] Waiting additional 3s...")
                        await trio.sleep(3)
                        protos = self.host.get_peerstore().get_protocols(info.peer_id)
                        proto_strs = [str(p) for p in protos] if protos else []
                        await self._send_system_message(f"[Relay] Relay protocols (retry): {proto_strs}")
                        if hop_proto in proto_strs:
                            await self._send_system_message(f"[Relay]  Relay NOW HAS {hop_proto}")
                        else:
                            await self._send_system_message(f"[Relay]  Relay STILL MISSING {hop_proto}")
                except Exception as e:
                    await self._send_system_message(f"[Relay] Could not read relay protocols: {e}")

                await self._send_system_message(
                    f"[Relay] Our circuit_v2: {self.circuit_v2 is not None}, "
                    f"allow_hop: {getattr(self.circuit_v2, 'allow_hop', 'N/A')}, "
                    f"relay_discovery: {self.relay_discovery is not None}"
                )

                if self.relay_discovery is not None:
                    # CRITICAL: Add relay to _discovered_relays BEFORE calling make_reservation
                    # make_reservation checks _discovered_relays and fails if peer is not there!
                    now = time.time()
                    self.relay_discovery._discovered_relays[info.peer_id] = RelayInfo(
                        peer_id=info.peer_id,
                        discovered_at=now,
                        last_seen=now,
                    )
                    await self._send_system_message(f"[Relay] Added relay {str(info.peer_id)[:12]} to discovered_relays")

                    await self._send_system_message(f"[Relay] Starting reservation process with relay {info.peer_id}...")
                    reserved = False
                    for attempt in range(1, 4):
                        await self._send_system_message(f"[Relay] Attempting reservation (try {attempt}/3)...")
                        with trio.move_on_after(15) as reserve_scope:
                            reserved = await self.relay_discovery.make_reservation(info.peer_id)

                        if reserve_scope.cancelled_caught:
                            await self._send_system_message(f"[Relay] ⏱Reservation timed out (attempt {attempt}/3)")
                            await trio.sleep(2 * attempt)
                            continue

                        if reserved:
                            await self._send_system_message(f"[Relay] Reservation GRANTED by {str(info.peer_id)[:12]} on attempt {attempt}/3")
                            await self._send_system_message(f"Relay active via {str(info.peer_id)[:12]}")
                            
                            # Log relay addresses
                            relay_addrs = [str(a) for a in self.host.get_addrs() if "p2p-circuit" in str(a)]
                            if relay_addrs:
                                await self._send_system_message(f"[Relay] Your relay address: {relay_addrs[0]}")
                            break
                        else:
                            await self._send_system_message(f"[Relay] Reservation DENIED by {str(info.peer_id)[:12]} (attempt {attempt}/3)")
                            await self._send_system_message(f"[Relay] Retrying in {2 * attempt}s...")
                            await trio.sleep(2 * attempt)

                    if not reserved:
                        await self._send_system_message(f"[Relay] All 3 reservation attempts FAILED for {str(info.peer_id)[:12]}")
                        await self._send_system_message(f"[Relay] Possible issues: relay not in HOP mode, relay overloaded, or protocol mismatch")
                else:
                    await self._send_system_message(f"[Relay] No RelayDiscovery available — skipping reservation")

            except Exception as e:
                await self._send_system_message(f"[Relay] Exception during relay setup: {type(e).__name__}: {e}")
                logger.exception(f"[Relay] Full traceback for {addr_str}:")


    async def _request_relay_reservation(self, relay_peer_id: ID):
        """
        Request a Circuit Relay v2 reservation via RelayDiscovery.make_reservation().
        Returns True on success (STATUS_OK from relay), False if denied.
        Failure is non-fatal — direct connections still work without a reservation.
        """
        if self.relay_discovery is None:
            logger.info(f"ℹNo RelayDiscovery — skipping reservation for {relay_peer_id}")
            return

        try:
            logger.info(f"📡 Requesting relay reservation from: {relay_peer_id}")
            # RelayDiscovery.make_reservation(peer_id: ID) -> bool
            success = await self.relay_discovery.make_reservation(relay_peer_id)
            if success:
                logger.info(f"Relay reservation granted by {relay_peer_id}")
                await self._send_system_message(
                    f"Relay reservation active via {str(relay_peer_id)[:12]}"
                )
            else:
                logger.warning(f"Relay {relay_peer_id} denied reservation")
                await self._send_system_message(
                    f"Relay reservation denied by {str(relay_peer_id)[:12]}"
                )
        except Exception as e:
            logger.warning(f"Relay reservation failed (non-fatal): {e}")

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
                    logger.info(f" Already connected to peer: {info.peer_id}, skipping connection attempt")
                    continue
                
                # Log connection attempt
                logger.info(f"Initiating connection to peer: {info.peer_id}")
                await self.host.connect(info)
                logger.info(f"TCP connection established to peer: {info.peer_id}")
                
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
                        logger.info(f"Active connections to peer {info.peer_id}: {len(connections)}")
                    else:
                        logger.info(f"No direct connection info available for peer {info.peer_id}")
                except Exception as conn_err:
                    logger.warning(f"Could not check connection status: {conn_err}")
                
                # Wait for PubSub protocol negotiation
                logger.info(f"Waiting for PubSub protocol negotiation...")
                await trio.sleep(3)
                
                # Check final PubSub status
                await self._check_pubsub_status(info.peer_id)
                
                await self._send_system_message(f"Connected to peer: {str(info.peer_id)[:8]}")
                
            except Exception as e:
                logger.error(f"Failed to connect to {addr_str}: {e}")
                await self._send_system_message(f"Failed to connect to {addr_str}: {e}")
    
    async def _inspect_peer_protocols(self, peer_id):
        """Inspect and log all protocols supported by a peer."""
        try:
            logger.info(f"Checking peerstore for peer: {peer_id}")
            peerstore = self.host.get_peerstore()
            
            # Check if we can access protocols - different py-libp2p versions have different APIs
            try:
                if hasattr(peerstore, 'get_protocols'):
                    protocols = peerstore.get_protocols(peer_id)
                elif hasattr(peerstore, 'protocols'):
                    protocols = peerstore.protocols(peer_id)
                else:
                    # Fallback - just log that we connected successfully
                    logger.info(f"Successfully connected to peer {peer_id}")
                    logger.info(f"Protocol inspection not available in this py-libp2p version")
                    return
                    
                if protocols:
                    logger.info(f"Peer {peer_id} supports {len(protocols)} protocols:")
                    for i, protocol in enumerate(protocols, 1):
                        logger.info(f"  {i}: {protocol}")
                        if "meshsub" in str(protocol) or "gossipsub" in str(protocol):
                            logger.info(f"  Found PubSub protocol: {protocol}")
                else:
                    logger.info(f" No protocols found for peer {peer_id} yet (may still be negotiating)")
                    
            except Exception as proto_err:
                logger.info(f"Protocol details not accessible: {proto_err}")
                logger.info(f"Peer {peer_id} connected successfully")
                    
        except Exception as e:
            logger.warning(f"Error inspecting peer protocols: {e}")
            logger.info(f"Peer {peer_id} connected successfully")
    
    async def _check_pubsub_status(self, peer_id):
        """Check the PubSub connection status with a specific peer."""
        try:
            logger.info(f"Checking PubSub status for peer: {peer_id}")
            pubsub_peers = list(self.pubsub.peers.keys())
            logger.info(f"Total PubSub peers: {len(pubsub_peers)}")
            for i, p in enumerate(pubsub_peers, 1):
                logger.info(f"  PubSub peer {i}: {p}")
            
            if peer_id in self.pubsub.peers:
                logger.info(f"Peer {peer_id} is in PubSub mesh")
                
                # Check GossipSub specific status
                if hasattr(self.pubsub, 'router') and hasattr(self.pubsub.router, 'mesh'):
                    mesh = self.pubsub.router.mesh
                    logger.info(f"GossipSub mesh status:")
                    logger.info(f"Mesh topics: {list(mesh.keys())}")
                    for topic, topic_peers in mesh.items():
                        logger.info(f"    Topic '{topic}': {len(topic_peers)} peers")
                        if peer_id in topic_peers:
                            logger.info(f"Peer {peer_id} is in mesh for topic '{topic}'")
                        else:
                            logger.warning(f"Peer {peer_id} is NOT in mesh for topic '{topic}'")
            else:
                logger.warning(f"Peer {peer_id} is NOT in PubSub mesh")
                logger.info("Possible reasons:")
                logger.info(" 1. PubSub protocol negotiation failed")
                logger.info(" 2. Peer doesn't support compatible GossipSub version")
                logger.info(" 3. Network issues preventing PubSub handshake")
                
        except Exception as e:
            logger.error(f"Error checking PubSub status: {e}")
    
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
                        await self._send_system_message(f"[Connect] Processing connection request: {multiaddr_str}")
                        
                        # Parse and connect to the peer
                        try:
                            # Parse the multiaddress
                            maddr = multiaddr.Multiaddr(multiaddr_str)
                            
                            # Try to get peer info from the multiaddress
                            peer_info = info_from_p2p_addr(maddr)
                            
                            if peer_info:
                                # Connect to the peer
                                await self._send_system_message(f"[Connect] Connecting to peer: {peer_info.peer_id}")
                                await self.host.connect(peer_info)
                                await self._send_system_message(f"[Connect] Successfully connected to peer: {peer_info.peer_id}")
                            else:
                                await self._send_system_message(f"[Connect] Invalid multiaddress format")
                                
                        except Exception as e:
                            await self._send_system_message(f"[Connect] Connection failed: {type(e).__name__}: {e}")
                            logger.exception(f"Full traceback for peer connection:")
                            
                except Empty:
                    # No request available, that's fine
                    await trio.sleep(0.1)  # Brief pause to avoid busy loop
                except Exception as e:
                    logger.error(f"Error processing peer connection: {e}")
                    await trio.sleep(0.1)
                    
            except Exception as e:
                logger.error(f"Error in peer connection processing: {e}")
                await trio.sleep(0.1)
    
    async def _process_relay_connections(self):
        """Process relay connection requests from UI."""
        
        # Initialize relay connection queue if needed
        if not hasattr(self, 'relay_connection_queue'):
            self.relay_connection_queue = janus.Queue()
        
        while self.running:
            try:
                # Check for relay connection requests (non-blocking)
                try:
                    relay_addr_str = self.relay_connection_queue.sync_q.get_nowait()
                    if relay_addr_str:
                        await self._send_system_message(f"[Relay] Processing dynamic relay connection: {relay_addr_str}")
                        
                        if not self.circuit_v2:
                            await self._send_system_message("[Relay] Relay not initialized (use --relay at startup)")
                            continue
                        
                        try:
                            # Parse the relay address
                            addr = multiaddr.Multiaddr(relay_addr_str)
                            info = info_from_p2p_addr(addr)
                            await self._send_system_message(f"[Relay] Parsed relay peer ID: {info.peer_id}")
                            
                            # Check if already connected
                            if info.peer_id in self.host.get_network().connections:
                                await self._send_system_message(f"[Relay] Already connected to {info.peer_id}")
                            else:
                                await self._send_system_message(f"[Relay] Connecting to relay...")
                                await self.host.connect(info)
                                await self._send_system_message(f"[Relay] TCP connection established")
                            
                            # Wait for protocol negotiation
                            await self._send_system_message(f"[Relay] Waiting for protocol negotiation...")
                            await trio.sleep(3)
                            
                            # Attempt reservation
                            if self.relay_discovery:
                                # CRITICAL: Add relay to _discovered_relays first!
                                now = time.time()
                                self.relay_discovery._discovered_relays[info.peer_id] = RelayInfo(
                                    peer_id=info.peer_id,
                                    discovered_at=now,
                                    last_seen=now,
                                )
                                await self._send_system_message(f"[Relay] Added relay to discovered_relays")
                                
                                await self._send_system_message(f"[Relay] Making reservation...")
                                reserved = await self.relay_discovery.make_reservation(info.peer_id)
                                
                                if reserved:
                                    await self._send_system_message(f"[Relay] Reservation GRANTED by {str(info.peer_id)[:12]}")
                                    
                                    # Show relay addresses
                                    relay_addrs = [str(a) for a in self.host.get_addrs() if "p2p-circuit" in str(a)]
                                    if relay_addrs:
                                        await self._send_system_message(f"[Relay] Your relay address: {relay_addrs[0]}")
                                else:
                                    await self._send_system_message(f"[Relay] Reservation DENIED")
                            else:
                                await self._send_system_message(f"[Relay] No RelayDiscovery service available")
                                
                        except Exception as e:
                            await self._send_system_message(f"[Relay] Failed: {type(e).__name__}: {e}")
                            logger.exception(f"Full traceback for relay connection:")
                            
                except Empty:
                    await trio.sleep(0.1)
                except Exception as e:
                    logger.error(f"Error processing relay connection: {e}")
                    await trio.sleep(0.1)
                    
            except Exception as e:
                logger.error(f"Error in relay connection processing: {e}")
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

        # Relay server stats: how many reservations we're hosting
        hosted_reservations = 0
        hosted_peers = []
        if self.relay_server_mode and self.circuit_v2 and hasattr(self.circuit_v2, 'resource_manager'):
            rm = self.circuit_v2.resource_manager
            if hasattr(rm, '_reservations'):
                hosted_reservations = len(rm._reservations)
                hosted_peers = [str(pid)[:12] for pid in rm._reservations.keys()]

        return {
            'peer_id': str(self.host.get_id()),
            'nickname': self.nickname,
            'multiaddr': self.full_multiaddr,
            'relay_addrs': relay_paths,
            'relay_server_mode': self.relay_server_mode,
            'hosted_reservations': hosted_reservations,
            'hosted_peers': hosted_peers,
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
    
    def connect_to_relay(self, relay_addr: str) -> bool:
        """
        Connect to a relay server dynamically (thread-safe wrapper).
        Requires --relay flag at startup to initialize relay components.
        
        Args:
            relay_addr: The multiaddress of the relay to connect to
            
        Returns:
            True if relay connection request was queued, False otherwise
        """
        if not self.host or not self.running:
            logger.warning("Cannot connect to relay: host not ready or service not running")
            return False
        
        if not self.circuit_v2:
            logger.warning("Cannot connect to relay: relay not initialized (use --relay at startup)")
            return False
        
        try:
            # Add to relay_addrs and trigger connection
            if relay_addr not in self.relay_addrs:
                self.relay_addrs.append(relay_addr)
            
            # Put relay connection request in a special queue
            if not hasattr(self, 'relay_connection_queue'):
                self.relay_connection_queue = janus.Queue()
            
            self.relay_connection_queue.sync_q.put(relay_addr)
            logger.info(f"Queued relay connection request: {relay_addr}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to queue relay connection: {e}")
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
                
                logger.info(f"Received identify info from {peer_id}")
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
            logger.error(f"Failed to get identify info from peer {peer_id}: {e}")
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
            logger.error(f"Failed to get peer info for {peer_id_str}: {e}")
        
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
