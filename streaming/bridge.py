"""Kafka to Pub/Sub streaming bridge.

This module re-exports the bridge from the orchestrator package.
The canonical implementation is in orchestrator/orchestrator/bridge.py.

For direct execution, use:
    python -m orchestrator.orchestrator.bridge
"""

# Note: The orchestrator package uses a nested structure (orchestrator/orchestrator/)
# so imports need the full path
from orchestrator.orchestrator.bridge import (
    BridgeConfig,
    BridgeError,
    BridgeMetrics,
    BufferedMessage,
    KafkaConnectionError,
    PubSubPublishError,
    StreamingBridge,
    main,
)

__all__ = [
    "BridgeConfig",
    "BridgeError",
    "BridgeMetrics",
    "BufferedMessage",
    "KafkaConnectionError",
    "PubSubPublishError",
    "StreamingBridge",
    "main",
]

if __name__ == "__main__":
    main()
