"""
Enhanced main entry point with agent timeout and graceful shutdown
"""
import asyncio
import logging
import signal
import sys
from config import (
    validate_environment, setup_logging,
    LIVEKIT_URL, CALLBACK_WS_URL, TELEPHONY_SAMPLE_RATE, LIVEKIT_SAMPLE_RATE
)
from server.websocket_server import WebSocketServerManager
from server.http_server import HTTPServerManager

logger = logging.getLogger(__name__)


class TelephonyLiveKitBridge:
    """Enhanced application with agent timeout and graceful shutdown"""
    
    def __init__(self):
        self.websocket_server = WebSocketServerManager()
        self.http_server = HTTPServerManager()
        self._shutdown_initiated = False
        self._server_tasks = []
        
    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        def signal_handler(signum, frame):
            if not self._shutdown_initiated:
                self._shutdown_initiated = True
                signal_name = signal.Signals(signum).name
                logger.info(f"📶 Received {signal_name} signal - initiating graceful shutdown...")
                
                # Get the current event loop
                try:
                    loop = asyncio.get_running_loop()
                    if not loop.is_closed():
                        # Schedule graceful shutdown
                        loop.create_task(self._graceful_shutdown())
                except RuntimeError:
                    logger.warning("No running event loop found for graceful shutdown")
                    sys.exit(1)
        
        # Handle common shutdown signals
        signal.signal(signal.SIGINT, signal_handler)   # Ctrl+C
        signal.signal(signal.SIGTERM, signal_handler)  # Docker/systemd stop
        
        # On Unix systems, also handle SIGHUP
        try:
            signal.signal(signal.SIGHUP, signal_handler)
        except AttributeError:
            # Windows doesn't have SIGHUP
            pass
        
        logger.info("✅ Signal handlers configured for graceful shutdown")
    
    async def _graceful_shutdown(self):
        """Perform graceful shutdown of the entire application"""
        logger.info("🛑 Starting application graceful shutdown...")
        
        try:
            # Step 1: Signal servers to stop accepting new connections
            logger.info("🚫 Stopping acceptance of new connections...")
            self.websocket_server.initiate_shutdown()
            self.http_server.initiate_shutdown()
            
            # Step 2: Give active calls time to complete or be cleaned up
            active_handlers = self.websocket_server.get_active_handler_count()
            if active_handlers > 0:
                logger.info(f"⏰ Waiting for {active_handlers} active calls to clean up...")
                
                # Wait up to 30 seconds for calls to clean up naturally
                for i in range(30):
                    current_handlers = self.websocket_server.get_active_handler_count()
                    if current_handlers == 0:
                        logger.info(f"✅ All calls completed after {i+1} seconds")
                        break
                    
                    if i % 5 == 0:  # Log every 5 seconds
                        logger.info(f"⏰ Still waiting for {current_handlers} calls to complete...")
                    
                    await asyncio.sleep(1)
                
                # Force cleanup any remaining handlers
                remaining = self.websocket_server.get_active_handler_count()
                if remaining > 0:
                    logger.warning(f"🔪 Force cleaning up {remaining} remaining handlers...")
                    await self.websocket_server.cleanup_all_handlers()
            
            # Step 3: Wait a moment for server tasks to complete
            logger.info("⏰ Waiting for servers to shutdown...")
            await asyncio.sleep(2)
            
            # Step 4: Cancel any remaining server tasks
            logger.info("🔄 Cancelling remaining server tasks...")
            for task in self._server_tasks:
                if not task.done():
                    task.cancel()
                    try:
                        await asyncio.wait_for(task, timeout=2.0)
                    except (asyncio.CancelledError, asyncio.TimeoutError):
                        pass
            
            logger.info("✅ Application graceful shutdown complete")
            
        except Exception as e:
            logger.error(f"❌ Error during graceful shutdown: {e}")
            import traceback
            traceback.print_exc()
        finally:
            # Force exit
            logger.info("🚪 Exiting application...")
            sys.exit(0)
    
    async def start(self):
        """Start the enhanced bridge application"""
        logger.info("🚀 Starting Enhanced Telephony-LiveKit Bridge...")
        logger.info("🎯 Features: 5-Second Agent Timeout + Graceful Shutdown")
        logger.info("=" * 80)
        
        # Setup signal handlers
        self._setup_signal_handlers()
        
        # Validate environment
        try:
            validate_environment()
            logger.info("✅ All environment variables configured")
        except ValueError as e:
            logger.error(f"❌ {e}")
            return
        
        # Log configuration
        self._log_configuration()
        
        try:
            # Create server tasks
            logger.info("🚀 Starting servers...")
            
            self._server_tasks = [
                asyncio.create_task(self.websocket_server.start_server(), name="websocket_server"),
                asyncio.create_task(self.http_server.start_server(), name="http_server")
            ]
            
            # Wait for servers to complete or shutdown signal
            done, pending = await asyncio.wait(
                self._server_tasks,
                return_when=asyncio.FIRST_COMPLETED
            )
            
            # If we reach here, one of the servers completed (probably due to shutdown)
            logger.info("🔄 Server task completed - cleaning up...")
            
            # Cancel remaining tasks
            for task in pending:
                task.cancel()
                try:
                    await asyncio.wait_for(task, timeout=5.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
            
        except Exception as e:
            logger.error(f"❌ Server error: {e}")
            import traceback
            traceback.print_exc()
            await self._graceful_shutdown()
    
    def _log_configuration(self):
        """Log application configuration"""
        logger.info(f"🔗 LiveKit URL: {LIVEKIT_URL}")
        logger.info(f"📞 WebSocket URL: {CALLBACK_WS_URL}")
        logger.info(f"🎵 Audio Config: Telephony({TELEPHONY_SAMPLE_RATE}Hz) <-> LiveKit({LIVEKIT_SAMPLE_RATE}Hz)")
        logger.info(f"⏰ Agent timeout: 5 seconds")
        logger.info(f"🛡️ Graceful shutdown: ENABLED")
        logger.info(f"🔄 Signal handlers: SIGINT, SIGTERM" + (", SIGHUP" if hasattr(signal, 'SIGHUP') else ""))
        logger.info("=" * 80)


async def main():
    """Main function to run the enhanced application"""
    # Setup logging
    setup_logging()
    
    logger.info("🎯 Enhanced Telephony-LiveKit Bridge v2.0")
    logger.info("🔧 Agent Timeout: 5s | Graceful Shutdown: ✅")
    
    # Create and start the bridge
    bridge = TelephonyLiveKitBridge()
    await bridge.start()


if __name__ == "__main__":
    try:
        # Run the application
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Received KeyboardInterrupt")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        logger.info("🔚 Application terminated")