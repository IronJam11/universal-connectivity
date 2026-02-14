# Constants
from libp2p.pubsub.gossipsub import PROTOCOL_ID, PROTOCOL_ID_V11

DISCOVERY_SERVICE_TAG = "universal-connectivity"
PROTOCOL_ID_LIST = [PROTOCOL_ID] # PROTOCOL_ID_V11 - giving a problem when using docker with py-libp2p nodes, so commenting out for now to maintain compatibility. Will investigate further.
DEFAULT_PORT = 9095
DEFAULT_RELAY_LIMIT_DURATION = 3600  # seconds to keep messages for replay to new subscribers
DEFAULT_RELAY_LIMIT_DATA_SIZE = 1024 * 1024 * 100  # 100 MB total data size for relayed messages
DEFAULT_RELAY_MAX_CIRCUIT_CONNS = 10  # Max concurrent circuits (relayed connections)
DEFAULT_RELAY_MAX_RESERVATIONS = 5  # Max concurrent reservations (for relay clients)


DEFAULT_GOSSIPSUB_DEGREE = 3
DEFAULT_GOSSIPSUB_DEGREE_LOW = 2
DEFAULT_GOSSIPSUB_DEGREE_HIGH = 4
DEFAULT_GOSSIPSUB_GOSSIP_WINDOW = 2  # seconds to wait before gossiping a message
DEFAULT_GOSSIPSUB_GOSSIP_HISTORY = 5  # number of messages to keep in history for gossiping
DEFAULT_GOSSIPSUB_HEARTBEAT_INITIAL_DELAY = 2.0  # seconds before first heartbeat
DEFAULT_GOSSIPSUB_HEARTBEAT_INTERVAL = 5.0  # seconds between heartbeats (lower for testing)

MAX_RESERVATION_ATTEMPTS = 3  # Max attempts for relay reservation


BOOTSTRAP_PEERS = [
    "/ip4/139.178.65.157/tcp/4001/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
    "/ip4/139.178.91.71/tcp/4001/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
    "/ip4/145.40.118.135/tcp/4001/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt"
    "/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
    "/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa", 
    "/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zp7ykQCj2gRNdrFeqQ1vG13rMb4sPS",
    "/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
    "/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ"
]
