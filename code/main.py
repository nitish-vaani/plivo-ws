"""
Main entry point for the Telephony-LiveKit Bridge
"""
import asyncio
import logging
from config import (
    validate_environment, setup_logging,
    LIVEKIT_URL, CALLBACK_WS_URL, TELEPHONY_SAMPLE_RATE, LIVEKIT_SAMPLE_RATE
)
from server.websocket_server import WebSocketServerManager
from server.http_server import HTTPServerManager

logger = logging.getLogger(__name__)


class TelephonyLiveKitBridge:
    """Main application class for the Telephony-LiveKit Bridge"""
    
    def __init__(self):
        self.websocket_server = WebSocketServerManager()
        self.http_server = HTTPServerManager()
    
    async def start(self):
        """Start the bridge application"""
        logger.info("🚀 Starting Telephony-LiveKit Bridge...")
        logger.info("=" * 60)
        
        # Validate environment
        try:
            validate_environment()
            logger.info("✅ All environment variables configured")
        except ValueError as e:
            logger.error(f"❌ {e}")
            return
        
        # Log configuration (without secrets)
        self._log_configuration()
        
        try:
            # Run both servers concurrently
            logger.info("🚀 Starting servers...")
            await asyncio.gather(
                self.websocket_server.start_server(),
                self.http_server.start_server()
            )
        except KeyboardInterrupt:
            logger.info("👋 Received shutdown signal")
            await self._cleanup()
        except Exception as e:
            logger.error(f"❌ Server error: {e}")
            import traceback
            traceback.print_exc()
            await self._cleanup()
    
    def _log_configuration(self):
        """Log application configuration"""
        logger.info(f"🔗 LiveKit URL: {LIVEKIT_URL}")
        logger.info(f"📞 WebSocket URL: {CALLBACK_WS_URL}")
        logger.info(f"🎵 Audio Config: Telephony({TELEPHONY_SAMPLE_RATE}Hz) <-> LiveKit({LIVEKIT_SAMPLE_RATE}Hz)")
        logger.info("=" * 60)
    
    async def _cleanup(self):
        """Clean up application resources"""
        logger.info("🧹 Starting application cleanup...")
        await self.websocket_server.cleanup_all_handlers()
        logger.info("✅ Application cleanup complete")


async def main():
    """Main function to run the application"""
    # Setup logging
    setup_logging()
    
    # Create and start the bridge
    bridge = TelephonyLiveKitBridge()
    await bridge.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Shutting down...")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()