"""
HTTP server for API endpoints and Plivo webhooks
"""
import asyncio
import time
import uuid
import logging
from aiohttp import web
from config import (
    TELEPHONY_SAMPLE_RATE, LIVEKIT_SAMPLE_RATE, CALLBACK_WS_URL,
    LIVEKIT_URL, LIVEKIT_API_KEY, LIVEKIT_API_SECRET,
    HTTP_HOST, HTTP_PORT
)
from agents.agent_manager import AgentManager

logger = logging.getLogger(__name__)


class HTTPServerManager:
    """Manages HTTP server and API endpoints"""
    
    def __init__(self):
        self.agent_manager = AgentManager()
        self.app = self._create_app()
    
    def _create_app(self):
        """Create web application with routes"""
        app = web.Application()
        
        # Health and utility endpoints
        app.router.add_get("/health", self._handle_health)
        app.router.add_post("/trigger", self._handle_trigger_room)
        
        # Plivo-specific endpoints
        app.router.add_get("/plivo-app/plivo.xml", self._handle_plivo_xml)
        app.router.add_post("/plivo-app/hangup", self._handle_plivo_hangup)
        app.router.add_get("/plivo-app/hangup", self._handle_plivo_hangup)
        app.router.add_post("/plivo-app/stream-status", self._handle_stream_status)
        app.router.add_get("/plivo-app/stream-status", self._handle_stream_status)
        app.router.add_post("/plivo-app/trigger-call", self._handle_trigger_call)
        
        return app
    
    async def _handle_health(self, request):
        """Health check endpoint"""
        return web.json_response({
            "status": "healthy",
            "timestamp": time.time(),
            "services": {
                "websocket": "running",
                "http": "running",
                "livekit": "configured" if all([LIVEKIT_URL, LIVEKIT_API_KEY, LIVEKIT_API_SECRET]) else "not configured"
            },
            "config": {
                "telephony_sample_rate": TELEPHONY_SAMPLE_RATE,
                "livekit_sample_rate": LIVEKIT_SAMPLE_RATE,
                "websocket_url": CALLBACK_WS_URL
            }
        })

    async def _handle_trigger_room(self, request):
        """Trigger agent in a specific room"""
        try:
            data = await request.json()
            room = data["room"]
            
            logger.info(f"🎯 Manual agent trigger for room: {room}")
            asyncio.create_task(self.agent_manager.trigger_agent(room))
            
            return web.json_response({
                "status": "triggered",
                "room": room,
                "message": f"Agent triggered for room {room}"
            })
        except Exception as e:
            logger.error(f"❌ Error triggering agent: {e}")
            return web.json_response({"error": str(e)}, status=400)

    async def _handle_plivo_xml(self, request):
        """Return Plivo XML for call flow"""
        try:
            # Get room name from query parameters
            room = request.query.get("room", f"plivo-room-{uuid.uuid4()}")
            logger.info(f"📋 Generating Plivo XML for room: {room}")
            
            # Plivo XML response for audio streaming
            response_text = f"""<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Stream 
        bidirectional="true" 
        keepCallAlive="true" 
        contentType="audio/x-mulaw;rate=8000"
        streamTimeout="3600"
        statusCallbackUrl="{request.url.scheme}://{request.host}/plivo-app/stream-status"
    >{CALLBACK_WS_URL}/?room={room}</Stream>
</Response>"""
            
            logger.info(f"📋 Returning Plivo XML for room: {room}")
            return web.Response(text=response_text, content_type="text/xml")
            
        except Exception as e:
            logger.error(f"❌ Error generating Plivo XML: {e}")
            return web.Response(
                text="<?xml version='1.0' encoding='UTF-8'?><Response><Hangup/></Response>", 
                content_type="text/xml", 
                status=500
            )

    async def _handle_plivo_hangup(self, request):
        """Handle Plivo hangup callback"""
        try:
            # Parse request data
            if request.method == 'POST':
                try:
                    data = await request.json()
                except:
                    data = dict(await request.post())
            else:
                data = dict(request.query)
            
            call_uuid = data.get('CallUUID', data.get('call_uuid', 'unknown'))
            hangup_cause = data.get('HangupCause', data.get('hangup_cause', 'unknown'))
            hangup_source = data.get('HangupSource', data.get('hangup_source', 'unknown'))
            call_duration = data.get('Duration', data.get('duration', '0'))
            
            logger.info(f"📞 HANGUP CALLBACK - Call: {call_uuid}")
            logger.info(f"   Cause: {hangup_cause}")
            logger.info(f"   Source: {hangup_source}")
            logger.info(f"   Duration: {call_duration}s")
            logger.info(f"   Full data: {data}")
            
            return web.Response(text="OK", status=200)
            
        except Exception as e:
            logger.error(f"❌ Error processing hangup callback: {e}")
            return web.Response(text="Error", status=500)

    async def _handle_stream_status(self, request):
        """Handle Plivo stream status callback"""
        try:
            if request.method == 'POST':
                try:
                    data = await request.json()
                except:
                    data = dict(await request.post())
            else:
                data = dict(request.query)
            
            stream_id = data.get('StreamId', data.get('stream_id', 'unknown'))
            call_uuid = data.get('CallUUID', data.get('call_uuid', 'unknown'))
            status = data.get('Status', data.get('status', 'unknown'))
            
            logger.info(f"📡 STREAM STATUS - Stream: {stream_id}")
            logger.info(f"   Call: {call_uuid}")
            logger.info(f"   Status: {status}")
            logger.info(f"   Full data: {data}")
            
            return web.Response(text="OK", status=200)
            
        except Exception as e:
            logger.error(f"❌ Error processing stream status: {e}")
            return web.Response(text="Error", status=500)

    async def _handle_trigger_call(self, request):
        """Trigger a new call via Plivo API - for testing purposes"""
        try:
            data = await request.json()
            to_number = data["to"]
            from_number = data["from"] 
            room = data.get("room", f"plivo-room-{uuid.uuid4()}")
            
            logger.info(f"📞 Triggering Plivo call: {from_number} -> {to_number} (room: {room})")
            
            # This would require Plivo credentials - implement if needed
            return web.json_response({
                "status": "not_implemented",
                "room": room,
                "message": f"Call triggering not implemented - add Plivo credentials and uncomment code"
            })
        except Exception as e:
            logger.error(f"❌ Error triggering call: {e}")
            return web.json_response({"error": str(e)}, status=400)
    
    async def start_server(self):
        """Start HTTP server"""
        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, HTTP_HOST, HTTP_PORT)
        await site.start()
        
        logger.info(f"🌐 HTTP server listening on http://{HTTP_HOST}:{HTTP_PORT}")
        logger.info(f"📋 Plivo XML endpoint: http://{HTTP_HOST}:{HTTP_PORT}/plivo-app/plivo.xml")
        logger.info(f"📞 Plivo hangup callback: http://{HTTP_HOST}:{HTTP_PORT}/plivo-app/hangup")