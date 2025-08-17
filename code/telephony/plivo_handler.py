"""
Plivo WebSocket message handling and protocol implementation
"""
import json
import base64
import logging
from config import TELEPHONY_SAMPLE_RATE, MESSAGE_LOG_FREQUENCY

logger = logging.getLogger(__name__)


class PlivoMessageHandler:
    """Handles Plivo WebSocket protocol and message processing"""
    
    def __init__(self):
        self.stream_sid = None
        self.call_active = False
        self.messages_received = 0
        self.messages_sent = 0
    
    async def handle_message(self, message, audio_callback=None, event_callback=None):
        """Handle incoming WebSocket message from Plivo"""
        self.messages_received += 1
        
        try:
            if isinstance(message, str):
                event = json.loads(message)
                await self._handle_telephony_event(event, audio_callback, event_callback)
            else:
                # Handle binary audio data directly
                if audio_callback:
                    await audio_callback(message)
                    
        except json.JSONDecodeError as e:
            logger.error(f"❌ Invalid JSON from Plivo: {e}")
            logger.error(f"Message content: {message[:100]}...")
    
    async def _handle_telephony_event(self, event, audio_callback=None, event_callback=None):
        """Handle Plivo WebSocket events"""
        event_type = event.get("event")
        
        if event_type == "start":
            await self._handle_start_event(event)
        elif event_type == "media":
            await self._handle_media_event(event, audio_callback)
        elif event_type == "stop":
            await self._handle_stop_event(event, event_callback)
        else:
            logger.info(f"❓ Unknown Plivo event: {event_type}")
            logger.info(f"📄 Event data: {json.dumps(event, indent=2)}")
    
    async def _handle_start_event(self, event):
        """Handle call start event"""
        logger.info("🟢 CALL STARTED")
        self.call_active = True
        
        start_data = event.get("start", {})
        self.stream_sid = start_data.get("streamId")
        call_id = start_data.get("callId")
        
        logger.info(f"📊 Stream ID: {self.stream_sid}")
        logger.info(f"📊 Call ID: {call_id}")
        logger.info(f"📊 Account ID: {start_data.get('accountId')}")
        logger.info(f"📊 Media Format: {start_data.get('mediaFormat')}")
        logger.info(f"📄 Full start event: {json.dumps(event, indent=2)}")
        
        # Critical check
        if self.stream_sid:
            logger.info(f"✅ Stream ID captured successfully: {self.stream_sid}")
        else:
            logger.error(f"❌ CRITICAL: No stream ID found in start event!")
            logger.error(f"❌ Start data keys: {list(start_data.keys())}")
    
    async def _handle_media_event(self, event, audio_callback=None):
        """Handle media/audio event"""
        # Only log Plivo event for first few or every 5 seconds
        if self.messages_received <= 10:
            logger.info(f"📞 Plivo event: media")
        
        media_data = event.get("media", {})
        payload = media_data.get("payload")
        track = media_data.get("track", "inbound")
        
        if payload and audio_callback:
            try:
                # Decode base64 audio data (μ-law format from Plivo)
                decoded_audio = base64.b64decode(payload)
                await audio_callback(decoded_audio)
            except Exception as e:
                logger.error(f"❌ Error processing Plivo media: {e}")
        elif not payload:
            if self.messages_received <= 10:
                logger.warning("⚠️ Media event without payload")
    
    async def _handle_stop_event(self, event, event_callback=None):
        """Handle call stop event"""
        logger.info("🔴 CALL ENDED")
        self.call_active = False
        
        if event_callback:
            await event_callback("call_ended")
    
    async def send_audio_to_plivo(self, websocket, audio_data):
        """Send audio data back to Plivo via WebSocket"""
        try:
            # Check WebSocket connection status
            try:
                websocket_closed = (not hasattr(websocket, 'open') or 
                                  not websocket.open if hasattr(websocket, 'open') else
                                  getattr(websocket, 'closed', False))
            except:
                websocket_closed = True
                
            if websocket_closed:
                logger.warning("⚠️ WebSocket closed, cannot send audio to Plivo")
                return False
                
            if not self.stream_sid:
                logger.error(f"❌ CRITICAL: No stream ID available! Cannot send audio to Plivo")
                logger.error(f"❌ Audio data size: {len(audio_data)} bytes - DROPPED")
                return False
                
            # Encode audio as base64 for Plivo
            encoded_audio = base64.b64encode(audio_data).decode('utf-8')
            
            # Create Plivo playAudio message format
            media_message = {
                "event": "playAudio",
                "media": {
                    "contentType": "audio/x-mulaw",
                    "sampleRate": TELEPHONY_SAMPLE_RATE,
                    "payload": encoded_audio
                }
            }
            
            await websocket.send(json.dumps(media_message))
            self.messages_sent += 1
            
            # Log success for first few messages
            if self.messages_sent <= 5:
                logger.info(f"📤 SUCCESS: Sent agent audio #{self.messages_sent} to Plivo ({len(audio_data)} bytes)")
            # Log occasionally for subsequent messages
            elif self.messages_sent % MESSAGE_LOG_FREQUENCY == 0:
                logger.info(f"📤 Sent {self.messages_sent} audio messages to Plivo")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error sending audio to Plivo: {e}")
            return False
    
    def get_call_stats(self):
        """Get call statistics"""
        return {
            "messages_received": self.messages_received,
            "messages_sent": self.messages_sent,
            "call_active": self.call_active,
            "stream_sid": self.stream_sid
        }
    
    def is_call_active(self):
        """Check if call is currently active"""
        return self.call_active
    
    def get_stream_id(self):
        """Get the current stream ID"""
        return self.stream_sid
