"""Base backend interface for stdiobus."""

from abc import ABC, abstractmethod
from typing import Callable, Optional

from stdiobus.types import BusState, BusStats


class Backend(ABC):
    """Abstract base class for stdiobus backends."""
    
    @abstractmethod
    async def start(self) -> None:
        """Start the backend."""
        ...
    
    @abstractmethod
    async def stop(self, timeout_sec: float = 30.0) -> None:
        """Stop the backend gracefully."""
        ...
    
    @abstractmethod
    def send(self, message: str) -> bool:
        """Send a message. Returns True if queued successfully."""
        ...
    
    @abstractmethod
    def on_message(self, handler: Callable[[str], None]) -> None:
        """Register a message handler."""
        ...
    
    @abstractmethod
    def get_state(self) -> BusState:
        """Get current state."""
        ...
    
    @abstractmethod
    def get_stats(self) -> BusStats:
        """Get statistics."""
        ...
    
    @abstractmethod
    def destroy(self) -> None:
        """Release all resources."""
        ...
    
    def is_running(self) -> bool:
        """Check if backend is running."""
        return self.get_state() == BusState.RUNNING
