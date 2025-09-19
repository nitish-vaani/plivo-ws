"""
Enhanced WebSocket server with graceful shutdown support
"""
import asyncio
import uuid
import logging
import websockets
from urllib.parse import urlparse, parse_qs
from config import WEBSOCKET_HOST, WEBSOCKET_PORT
from telephony.websocket_handler import TelephonyWebSocketHandler

logger = logging.getLogger(__name__)


class WebSocketServerManager:
    """Enhanced WebSocket server manager with graceful shutdown"""
    
    def __init__(self):
        self.active_handlers = []
        self.server = None
        self.shutdown_event = asyncio.Event()
        self._shutdown_initiated = False
    
    # async def handle_telephony_websocket(self, websocket, path):
    #     """Handle incoming WebSocket connections from Plivo"""
    #     handler = None
    #     try:
    #         logger.info(f"🔗 NEW PLIVO WEBSOCKET CONNECTION")
    #         logger.info(f"📍 Function parameter 'path': {path}")
            
    #         # The REAL path with parameters is in websocket.request.path!
    #         if hasattr(websocket, 'request') and hasattr(websocket.request, 'path'):
    #             actual_path = websocket.request.path
    #             logger.info(f"✅ Found full path in websocket.request.path: {actual_path}")
    #         else:
    #             actual_path = path
    #             logger.info(f"⚠️ Using fallback path: {actual_path}")
            
    #         # Parse room name and agent name from query parameters
    #         parsed_url = urlparse(actual_path)
    #         query = parse_qs(parsed_url.query)
            
    #         logger.info(f"📍 Parsed URL components:")
    #         logger.info(f"     path: {parsed_url.path}")
    #         logger.info(f"     query: {parsed_url.query}")
            
    #         # Extract parameters
    #         room_name = query.get("room", [f"plivo-room-{uuid.uuid4()}"])[0]
    #         agent_name = query.get("agent", [None])[0]
    #         outbound_agent_exists = query.get("outbound_agent_exists", ["false"])[0].lower() == "true"

    #         if outbound_agent_exists:
    #             logger.info(f"OUTBOUND CALL DETECTED - Agent already exists in room {room_name}")
    #         else:
    #             logger.info(f"INBOUND CALL DETECTED - Will create new agent for room {room_name}")
            
    #         logger.info(f"📞 Room: {room_name}")
    #         logger.info(f"🤖 Raw agent parameter: {query.get('agent')}")
    #         logger.info(f"🤖 Parsed agent name: {agent_name}")
            
    #         if agent_name:
    #             logger.info(f"🤖 SUCCESS! Will use CUSTOM agent: '{agent_name}'")
    #         else:
    #             logger.info(f"🤖 No custom agent, will use DEFAULT")
            
    #         # Parse noise settings from URL
    #         noise_settings = {}
    #         if "bg_noise" in query:
    #             noise_settings["enabled"] = query["bg_noise"][0].lower() == "true"
    #             logger.info(f"🔊 Background noise enabled: {noise_settings['enabled']}")
    #         if "noise_type" in query:
    #             noise_settings["noise_type"] = query["noise_type"][0]
    #             logger.info(f"🔊 Noise type: {noise_settings['noise_type']}")
    #         if "noise_volume" in query:
    #             try:
    #                 noise_settings["volume"] = float(query["noise_volume"][0])
    #                 logger.info(f"🔊 Noise volume: {noise_settings['volume']}")
    #             except ValueError:
    #                 logger.warning(f"⚠️ Invalid noise volume: {query['noise_volume'][0]}")
            
    #         # Show all parsed query parameters
    #         logger.info(f"📋 All parsed query parameters:")
    #         for key, value in query.items():
    #             logger.info(f"     {key}: {value}")

    #         # Create handler
    #         handler = TelephonyWebSocketHandler(room_name, websocket, agent_name, noise_settings)
            
    #         # ADD THIS: Set the outbound flag
    #         handler.outbound_agent_exists = outbound_agent_exists
            
    #         # Check if server is shutting down
    #         if self._shutdown_initiated:
    #             logger.warning("🚫 Server is shutting down - rejecting new connection")
    #             await websocket.close(code=1012, reason="Server shutting down")
    #             return
            
    #         # Create handler for Plivo WebSocket
    #         logger.info(f"🆕 Creating handler with agent_name='{agent_name}'")
    #         handler = TelephonyWebSocketHandler(room_name, websocket, agent_name, noise_settings)
    #         self.active_handlers.append(handler)
            
    #         # Initialize and handle messages
    #         message_task = await handler.initialize()
    #         if message_task:  # Only wait if initialization succeeded
    #             await message_task

    #     except Exception as e:
    #         logger.error(f"❌ Error in Plivo WebSocket handler: {e}")
    #         import traceback
    #         traceback.print_exc()
    #         try:
    #             if not websocket.closed:
    #                 await websocket.close(code=1011, reason=str(e))
    #         except:
    #             pass
    #     finally:
    #         # Remove handler from active list
    #         if handler and handler in self.active_handlers:
    #             self.active_handlers.remove(handler)
    #             logger.info(f"🧹 Removed handler from active list - {len(self.active_handlers)} handlers remaining")

    async def handle_telephony_websocket(self, websocket, path):
        """Handle incoming WebSocket connections from Plivo"""
        handler = None
        try:
            logger.info(f"🔗 NEW PLIVO WEBSOCKET CONNECTION")
            logger.info(f"📍 Function parameter 'path': {path}")
            
            # The REAL path with parameters is in websocket.request.path!
            if hasattr(websocket, 'request') and hasattr(websocket.request, 'path'):
                actual_path = websocket.request.path
                logger.info(f"✅ Found full path in websocket.request.path: {actual_path}")
            else:
                actual_path = path
                logger.info(f"⚠️ Using fallback path: {actual_path}")
            
            # Parse room name and agent name from query parameters
            parsed_url = urlparse(actual_path)
            query = parse_qs(parsed_url.query)
            
            logger.info(f"📍 Parsed URL components:")
            logger.info(f"     path: {parsed_url.path}")
            logger.info(f"     query: {parsed_url.query}")
            
            # Extract parameters
            room_name = query.get("room", [f"plivo-room-{uuid.uuid4()}"])[0]
            agent_name = query.get("agent", [None])[0]
            outbound_agent_exists = query.get("outbound_agent_exists", ["false"])[0].lower() == "true"

            if outbound_agent_exists:
                logger.info(f"OUTBOUND CALL DETECTED - Agent already exists in room {room_name}")
            else:
                logger.info(f"INBOUND CALL DETECTED - Will create new agent for room {room_name}")
            
            logger.info(f"📞 Room: {room_name}")
            logger.info(f"🤖 Raw agent parameter: {query.get('agent')}")
            logger.info(f"🤖 Parsed agent name: {agent_name}")
            
            if agent_name:
                logger.info(f"🤖 SUCCESS! Will use CUSTOM agent: '{agent_name}'")
            else:
                logger.info(f"🤖 No custom agent, will use DEFAULT")
            
            # Parse noise settings from URL
            noise_settings = {}
            if "bg_noise" in query:
                noise_settings["enabled"] = query["bg_noise"][0].lower() == "true"
                logger.info(f"🔊 Background noise enabled: {noise_settings['enabled']}")
            if "noise_type" in query:
                noise_settings["noise_type"] = query["noise_type"][0]
                logger.info(f"🔊 Noise type: {noise_settings['noise_type']}")
            if "noise_volume" in query:
                try:
                    noise_settings["volume"] = float(query["noise_volume"][0])
                    logger.info(f"🔊 Noise volume: {noise_settings['volume']}")
                except ValueError:
                    logger.warning(f"⚠️ Invalid noise volume: {query['noise_volume'][0]}")
            
            # Show all parsed query parameters
            logger.info(f"📋 All parsed query parameters:")
            for key, value in query.items():
                logger.info(f"     {key}: {value}")

            # Check if server is shutting down
            if self._shutdown_initiated:
                logger.warning("🚫 Server is shutting down - rejecting new connection")
                await websocket.close(code=1012, reason="Server shutting down")
                return
            
            # Create handler for Plivo WebSocket (ONLY ONCE)
            logger.info(f"🆕 Creating handler with agent_name='{agent_name}'")
            handler = TelephonyWebSocketHandler(room_name, websocket, agent_name, noise_settings)
            
            # Set the outbound flag
            handler.outbound_agent_exists = outbound_agent_exists
            
            self.active_handlers.append(handler)
            
            # Initialize and handle messages
            message_task = await handler.initialize()
            if message_task:  # Only wait if initialization succeeded
                await message_task

        except Exception as e:
            logger.error(f"❌ Error in Plivo WebSocket handler: {e}")
            import traceback
            traceback.print_exc()
            try:
                if not websocket.closed:
                    await websocket.close(code=1011, reason=str(e))
            except:
                pass
        finally:
            # Remove handler from active list
            if handler and handler in self.active_handlers:
                self.active_handlers.remove(handler)
                logger.info(f"🧹 Removed handler from active list - {len(self.active_handlers)} handlers remaining")


    async def start_server(self):
        """Start the WebSocket server"""
        logger.info(f"🌐 WebSocket server starting on ws://{WEBSOCKET_HOST}:{WEBSOCKET_PORT}")
        
        async def websocket_handler(websocket):
            try:
                path = websocket.path if hasattr(websocket, 'path') else "/"
                await self.handle_telephony_websocket(websocket, path)
            except Exception as e:
                logger.error(f"❌ Error in websocket handler: {e}")
        
        # Start the server
        self.server = await websockets.serve(
            websocket_handler, 
            WEBSOCKET_HOST, 
            WEBSOCKET_PORT
        )
        
        logger.info(f"✅ WebSocket server listening on ws://{WEBSOCKET_HOST}:{WEBSOCKET_PORT}")
        logger.info("🔧 Ready for Plivo WebSocket connections")
        logger.info(f"📋 Plivo should connect to: ws://sbi.vaaniresearch.com:{WEBSOCKET_PORT}/?room=your_room_name")
        
        # Wait for shutdown signal
        await self.shutdown_event.wait()
        
        # Start graceful shutdown
        await self._graceful_shutdown()
    
    async def _graceful_shutdown(self):
        """Perform graceful shutdown of WebSocket server"""
        self._shutdown_initiated = True
        logger.info("🛑 Starting WebSocket server graceful shutdown...")
        
        try:
            # Step 1: Stop accepting new connections
            if self.server:
                logger.info("🚫 Closing WebSocket server (no new connections)")
                self.server.close()
                await self.server.wait_closed()
                logger.info("✅ WebSocket server closed")
            
            # Step 2: Clean up all active handlers
            await self.cleanup_all_handlers()
            
        except Exception as e:
            logger.error(f"❌ Error during WebSocket server graceful shutdown: {e}")
        finally:
            logger.info("✅ WebSocket server graceful shutdown complete")
    
    async def cleanup_all_handlers(self):
        """Clean up all active handlers with timeout"""
        active_handlers = self.active_handlers.copy()
        
        if not active_handlers:
            logger.info("✅ No active WebSocket handlers to clean up")
            return
        
        logger.info(f"🧹 Cleaning up {len(active_handlers)} active WebSocket handlers...")
        
        cleanup_tasks = []
        for handler in active_handlers:
            if hasattr(handler, 'cleanup'):
                cleanup_tasks.append(handler.cleanup())
        
        if cleanup_tasks:
            try:
                # Wait for all cleanups with timeout
                await asyncio.wait_for(
                    asyncio.gather(*cleanup_tasks, return_exceptions=True),
                    timeout=30.0  # 30 seconds for all handlers
                )
                logger.info("✅ All WebSocket handlers cleaned up successfully")
                
            except asyncio.TimeoutError:
                logger.warning("⏰ WebSocket handler cleanup timed out after 30s")
                logger.warning("🔪 Some handlers may not have been cleaned up properly")
        
        # Clear the handlers list
        self.active_handlers.clear()
        logger.info("✅ WebSocket handler list cleared")
    
    def initiate_shutdown(self):
        """Initiate graceful shutdown from external signal"""
        if not self._shutdown_initiated:
            logger.info("📶 WebSocket server shutdown initiated")
            self.shutdown_event.set()
    
    def get_active_handler_count(self):
        """Get number of active handlers"""
        return len(self.active_handlers)
    
    def is_shutting_down(self):
        """Check if server is shutting down"""
        return self._shutdown_initiated