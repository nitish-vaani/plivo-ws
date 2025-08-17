# """
# WebSocket server for Plivo telephony connections
# """
# import asyncio
# import uuid
# import logging
# import websockets
# from urllib.parse import urlparse, parse_qs
# from config import WEBSOCKET_HOST, WEBSOCKET_PORT
# from telephony.websocket_handler import TelephonyWebSocketHandler

# logger = logging.getLogger(__name__)


# class WebSocketServerManager:
#     """Manages WebSocket server for telephony connections"""
    
#     def __init__(self):
#         self.active_handlers = []
    
#     async def handle_telephony_websocket(self, websocket, path):
#         """Handle incoming WebSocket connections from Plivo"""
#         handler = None
#         try:
#             logger.info(f"🔗 NEW PLIVO WEBSOCKET CONNECTION")
#             logger.info(f"📍 Path: {path}")
            
#             # Parse room name from query parameters
#             parsed_url = urlparse(path)
#             query = parse_qs(parsed_url.query)
#             room_name = query.get("room", [f"plivo-room-{uuid.uuid4()}"])[0]
            
#             logger.info(f"📞 Room: {room_name}")
            
#             # Create handler for Plivo WebSocket
#             handler = TelephonyWebSocketHandler(room_name, websocket)
#             self.active_handlers.append(handler)
            
#             # Initialize and handle messages
#             message_task = await handler.initialize()
#             await message_task

#         except Exception as e:
#             logger.error(f"❌ Error in Plivo WebSocket handler: {e}")
#             import traceback
#             traceback.print_exc()
#             try:
#                 if not websocket.closed:
#                     await websocket.close(code=1011, reason=str(e))
#             except:
#                 pass
#         finally:
#             # Remove handler from active list
#             if handler and handler in self.active_handlers:
#                 self.active_handlers.remove(handler)

#     async def start_server(self):
#         """Start the WebSocket server for Plivo connections"""
#         logger.info(f"🌐 WebSocket server starting on ws://{WEBSOCKET_HOST}:{WEBSOCKET_PORT}")
        
#         async def websocket_handler(websocket):
#             try:
#                 path = websocket.path if hasattr(websocket, 'path') else "/"
#                 await self.handle_telephony_websocket(websocket, path)
#             except Exception as e:
#                 logger.error(f"❌ Error in websocket handler: {e}")
        
#         # Start the server
#         async with websockets.serve(websocket_handler, WEBSOCKET_HOST, WEBSOCKET_PORT):
#             logger.info(f"✅ WebSocket server listening on ws://{WEBSOCKET_HOST}:{WEBSOCKET_PORT}")
#             logger.info("🔧 Ready for Plivo WebSocket connections")
#             logger.info(f"📋 Plivo should connect to: ws://sbi.vaaniresearch.com:{WEBSOCKET_PORT}/?room=your_room_name")
#             await asyncio.Future()
    
#     async def cleanup_all_handlers(self):
#         """Clean up all active handlers"""
#         logger.info(f"🧹 Cleaning up {len(self.active_handlers)} active handlers...")
#         cleanup_tasks = []
        
#         for handler in self.active_handlers.copy():
#             if hasattr(handler, 'cleanup'):
#                 cleanup_tasks.append(handler.cleanup())
        
#         if cleanup_tasks:
#             await asyncio.gather(*cleanup_tasks, return_exceptions=True)
        
#         self.active_handlers.clear()
#         logger.info("✅ All handlers cleaned up")


"""
WebSocket server for Plivo telephony connections
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
    """Manages WebSocket server for telephony connections"""
    
    def __init__(self):
        self.active_handlers = []
    
    async def handle_telephony_websocket(self, websocket, path):
        """Handle incoming WebSocket connections from Plivo"""
        handler = None
        try:
            logger.info(f"🔗 NEW PLIVO WEBSOCKET CONNECTION")
            logger.info(f"📍 Path: {path}")
            
            # Parse room name and agent name from query parameters
            parsed_url = urlparse(path)
            query = parse_qs(parsed_url.query)
            room_name = query.get("room", [f"plivo-room-{uuid.uuid4()}"])[0]
            agent_name = query.get("agent", [None])[0]  # Get agent name from URL
            
            # Parse noise settings from URL
            noise_settings = {}
            if "bg_noise" in query:
                noise_settings["enabled"] = query["bg_noise"][0].lower() == "true"
            if "noise_type" in query:
                noise_settings["noise_type"] = query["noise_type"][0]
            if "noise_volume" in query:
                try:
                    noise_settings["volume"] = float(query["noise_volume"][0])
                except ValueError:
                    logger.warning(f"⚠️ Invalid noise volume: {query['noise_volume'][0]}")
            
            logger.info(f"📞 Room: {room_name}")
            if agent_name:
                logger.info(f"🤖 Agent: {agent_name}")
            if noise_settings:
                logger.info(f"🔊 Noise settings: {noise_settings}")
            
            # Create handler for Plivo WebSocket
            handler = TelephonyWebSocketHandler(room_name, websocket, agent_name, noise_settings)
            self.active_handlers.append(handler)
            
            # Initialize and handle messages
            message_task = await handler.initialize()
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

    async def start_server(self):
        """Start the WebSocket server for Plivo connections"""
        logger.info(f"🌐 WebSocket server starting on ws://{WEBSOCKET_HOST}:{WEBSOCKET_PORT}")
        
        async def websocket_handler(websocket):
            try:
                path = websocket.path if hasattr(websocket, 'path') else "/"
                await self.handle_telephony_websocket(websocket, path)
            except Exception as e:
                logger.error(f"❌ Error in websocket handler: {e}")
        
        # Start the server
        async with websockets.serve(websocket_handler, WEBSOCKET_HOST, WEBSOCKET_PORT):
            logger.info(f"✅ WebSocket server listening on ws://{WEBSOCKET_HOST}:{WEBSOCKET_PORT}")
            logger.info("🔧 Ready for Plivo WebSocket connections")
            logger.info(f"📋 Plivo should connect to: ws://sbi.vaaniresearch.com:{WEBSOCKET_PORT}/?room=your_room_name")
            await asyncio.Future()
    
    async def cleanup_all_handlers(self):
        """Clean up all active handlers"""
        logger.info(f"🧹 Cleaning up {len(self.active_handlers)} active handlers...")
        cleanup_tasks = []
        
        for handler in self.active_handlers.copy():
            if hasattr(handler, 'cleanup'):
                cleanup_tasks.append(handler.cleanup())
        
        if cleanup_tasks:
            await asyncio.gather(*cleanup_tasks, return_exceptions=True)
        
        self.active_handlers.clear()
        logger.info("✅ All handlers cleaned up")