"""
LiveKit room management and connection handling
"""
import asyncio
import uuid
import logging
from livekit import rtc, api
from config import (
    LIVEKIT_URL, LIVEKIT_API_KEY, LIVEKIT_API_SECRET, 
    PARTICIPANT_NAME, LIVEKIT_CONNECTION_TIMEOUT
)

logger = logging.getLogger(__name__)


class LiveKitManager:
    """Manages LiveKit room connections and operations"""
    
    def __init__(self, room_name):
        self.room_name = room_name
        self.room = None
        self.connected = False
        self.identity = f"telephony-{uuid.uuid4()}"
        
    async def create_room_if_not_exists(self):
        """Create LiveKit room if it doesn't exist"""
        try:
            lkapi = api.LiveKitAPI(LIVEKIT_URL, LIVEKIT_API_KEY, LIVEKIT_API_SECRET)
            await lkapi.room.create_room(api.CreateRoomRequest(name=self.room_name))
            logger.info(f"✅ Created LiveKit room: {self.room_name}")
            await lkapi.aclose()
        except Exception as e:
            logger.info(f"ℹ️ Room creation result (may already exist): {e}")
    
    def create_access_token(self):
        """Create access token for LiveKit room"""
        return (api.AccessToken(LIVEKIT_API_KEY, LIVEKIT_API_SECRET)
                .with_identity(self.identity)
                .with_name(PARTICIPANT_NAME)
                .with_grants(api.VideoGrants(
                    room_join=True,
                    room=self.room_name,
                )))
    
    async def connect_to_room(self, event_handlers=None):
        """Connect to LiveKit room with optional event handlers"""
        try:
            logger.info(f"🔗 Connecting to LiveKit room: {self.room_name}")
            
            # Create room if needed
            await self.create_room_if_not_exists()
            
            # Create access token
            token = self.create_access_token()
            
            # Create room instance
            self.room = rtc.Room()
            
            # Setup event handlers if provided
            if event_handlers:
                self._setup_event_handlers(event_handlers)
            
            # Connect to room with timeout
            logger.info(f"⏰ Connecting to LiveKit room with {LIVEKIT_CONNECTION_TIMEOUT}s timeout...")
            await asyncio.wait_for(
                self.room.connect(LIVEKIT_URL, token.to_jwt()), 
                timeout=LIVEKIT_CONNECTION_TIMEOUT
            )
            
            logger.info(f"✅ LiveKit room connection successful!")
            self.connected = True
            
            return True
            
        except asyncio.TimeoutError:
            logger.error(f"❌ LiveKit connection timeout after {LIVEKIT_CONNECTION_TIMEOUT} seconds")
            return False
        except Exception as e:
            logger.error(f"❌ Failed to connect to LiveKit: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def _setup_event_handlers(self, handlers):
        """Setup room event handlers"""
        if 'on_connected' in handlers:
            self.room.on("connected")(handlers['on_connected'])
        if 'on_disconnected' in handlers:
            self.room.on("disconnected")(handlers['on_disconnected'])
        if 'on_participant_connected' in handlers:
            self.room.on("participant_connected")(handlers['on_participant_connected'])
        if 'on_participant_disconnected' in handlers:
            self.room.on("participant_disconnected")(handlers['on_participant_disconnected'])
        if 'on_track_published' in handlers:
            self.room.on("track_published")(handlers['on_track_published'])
        if 'on_track_subscribed' in handlers:
            self.room.on("track_subscribed")(handlers['on_track_subscribed'])
        if 'on_track_unsubscribed' in handlers:
            self.room.on("track_unsubscribed")(handlers['on_track_unsubscribed'])
    
    async def publish_audio_track(self, audio_track):
        """Publish audio track to LiveKit room"""
        if not self.room or not self.connected:
            logger.error("❌ Cannot publish track: not connected to room")
            return None
            
        try:
            options = rtc.TrackPublishOptions()
            options.source = rtc.TrackSource.SOURCE_MICROPHONE
            
            publication = await self.room.local_participant.publish_track(
                audio_track,
                options
            )
            logger.info(f"✅ Audio track published: {publication.sid}")
            return publication
        except Exception as e:
            logger.error(f"❌ Error publishing audio track: {e}")
            return None
    
    async def list_participants(self):
        """List participants in the room"""
        try:
            lkapi = api.LiveKitAPI(LIVEKIT_URL, LIVEKIT_API_KEY, LIVEKIT_API_SECRET)
            participants = await lkapi.room.list_participants(
                api.ListParticipantsRequest(room=self.room_name)
            )
            await lkapi.aclose()
            return participants.participants
        except Exception as e:
            logger.error(f"❌ Error listing participants: {e}")
            return []
    
    async def disconnect(self):
        """Disconnect from LiveKit room"""
        try:
            if self.room and self.connected:
                self.connected = False
                await asyncio.wait_for(self.room.disconnect(), timeout=3.0)
                logger.info("✅ Disconnected from LiveKit room")
        except asyncio.TimeoutError:
            logger.warning("⚠️ LiveKit disconnect timeout")
        except Exception as e:
            logger.error(f"❌ Error disconnecting from LiveKit: {e}")
    
    def is_connected(self):
        """Check if connected to LiveKit room"""
        return self.connected and self.room is not None
    
    def get_room(self):
        """Get the LiveKit room instance"""
        return self.room
    
    def get_remote_participants(self):
        """Get remote participants in the room"""
        if self.room:
            return self.room.remote_participants
        return {}