import asyncio
import websockets
import json
import logging
import uuid
import os
import requests
from urllib.parse import urlparse, parse_qs
from aiohttp import web
import base64
from livekit import rtc, api
import subprocess
import time
import struct
import audioop
import numpy as np


# Environment variables
from dotenv import load_dotenv
import os
load_dotenv()
LIVEKIT_URL = os.environ.get("LIVEKIT_URL")
LIVEKIT_API_KEY = os.environ.get("LIVEKIT_API_KEY")
LIVEKIT_API_SECRET = os.environ.get("LIVEKIT_API_SECRET")
PARTICIPANT_NAME = "Telephony Caller"
TELEPHONY_SAMPLE_RATE = 8000
LIVEKIT_SAMPLE_RATE = 48000
CALLBACK_WS_URL = os.environ.get("CALLBACK_WS_URL", "ws://0.0.0.0:8765")
agent_name = "Mysyara Agent" #outbound-caller / Mysyara Agent

# Configure detailed logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TelephonyAudioSource(rtc.AudioSource):
    """Audio source for processing telephony μ-law audio"""
    
    def __init__(self):
        super().__init__(
            sample_rate=LIVEKIT_SAMPLE_RATE,
            num_channels=1
        )
        
        self.resampler = rtc.AudioResampler(
            input_rate=TELEPHONY_SAMPLE_RATE,
            output_rate=LIVEKIT_SAMPLE_RATE,
            num_channels=1,
            quality=rtc.AudioResamplerQuality.HIGH
        )
        self.frame_count = 0
        self.total_bytes_processed = 0
        self.last_audio_time = time.time()
        
        logger.info(f"🎤 Audio Source initialized: {TELEPHONY_SAMPLE_RATE}Hz -> {LIVEKIT_SAMPLE_RATE}Hz")

    async def push_audio_data(self, mulaw_data):
        """Process μ-law audio data from telephony system"""
        try:
            if not mulaw_data:
                logger.warning("⚠️ Received empty audio data")
                return

            self.frame_count += 1
            self.total_bytes_processed += len(mulaw_data)
            self.last_audio_time = time.time()
            
            # Log audio data info much less frequently
            if self.frame_count % 250 == 0:
                logger.info(f"🎵 [INCOMING] Frame #{self.frame_count}: {len(mulaw_data)} bytes μ-law, "
                           f"Total: {self.total_bytes_processed} bytes")

            # Convert μ-law to 16-bit PCM
            try:
                pcm_data = audioop.ulaw2lin(mulaw_data, 2)  # 2 bytes per sample (16-bit)
            except Exception as e:
                logger.error(f"❌ μ-law conversion error: {e}, data size: {len(mulaw_data)}")
                return
            
            # Convert to samples array
            import array
            samples = array.array("h")  # signed short (16-bit)
            samples.frombytes(pcm_data)

            input_samples = len(samples)
            if input_samples == 0:
                logger.warning("⚠️ No samples after PCM conversion")
                return

            # Log sample info for first few frames only
            if self.frame_count <= 5:
                logger.info(f"🔍 Frame {self.frame_count}: {input_samples} samples, "
                           f"first few: {samples[:min(5, len(samples))]}")

            # Create input frame for resampling
            input_frame = rtc.AudioFrame.create(
                sample_rate=TELEPHONY_SAMPLE_RATE,
                num_channels=1,
                samples_per_channel=input_samples
            )
            input_frame.data[:input_samples] = samples

            # Resample to LiveKit's sample rate
            resampled_frames = self.resampler.push(input_frame)

            # Push each resampled frame to LiveKit
            for i, resampled_frame in enumerate(resampled_frames):
                await self.capture_frame(resampled_frame)
                
                if self.frame_count <= 5:
                    logger.info(f"🔍 Pushed resampled frame {i}: {resampled_frame.samples_per_channel} samples")

        except Exception as e:
            logger.error(f"❌ Error processing telephony audio frame {self.frame_count}: {e}")
            import traceback
            traceback.print_exc()

    def get_stats(self):
        """Get audio processing statistics"""
        return {
            "frames_processed": self.frame_count,
            "total_bytes": self.total_bytes_processed,
            "last_audio_ago": time.time() - self.last_audio_time,
            "avg_bytes_per_frame": self.total_bytes_processed / max(1, self.frame_count)
        }

    async def cleanup(self):
        """Clean up audio source"""
        try:
            stats = self.get_stats()
            logger.info(f"🧹 Audio source cleanup - Stats: {stats}")
            
            if hasattr(self.resampler, 'aclose'):
                await self.resampler.aclose()
            elif hasattr(self.resampler, 'close'):
                self.resampler.close()
                
        except Exception as e:
            logger.error(f"❌ Error cleaning up audio source: {e}")


class TelephonyWebSocketHandler:
    """WebSocket handler for telephony system integration"""
    
    def __init__(self, room_name, websocket):
        self.room_name = room_name
        self.websocket = websocket
        self.room = None
        self.audio_source = None
        self.audio_track = None
        self.connected = False
        self.agent_participant = None
        self.connection_start_time = time.time()
        self.messages_received = 0
        self.messages_sent = 0
        self.audio_stream_task = None
        # WebSocket handler variable to store stream ID for Plivo
        self.stream_sid = None
        
        # Track participants and their audio tracks
        self.participants = {}
        self.audio_tracks = {}
        
        # Audio conversion for return path
        self.return_resampler = rtc.AudioResampler(
            input_rate=LIVEKIT_SAMPLE_RATE,
            output_rate=TELEPHONY_SAMPLE_RATE,
            num_channels=1,
            quality=rtc.AudioResamplerQuality.HIGH
        )
        
        # Statistics
        self.stats = {
            "audio_frames_sent_to_livekit": 0,
            "audio_frames_received_from_agent": 0,
            "bytes_from_telephony": 0,
            "bytes_to_telephony": 0,
        }
        
        logger.info(f"🆕 Created telephony WebSocket handler for room: {room_name}")
        
    async def connect_to_livekit(self):
        """Connect to LiveKit room and setup audio track"""
        identity = f"telephony-{uuid.uuid4()}"
        
        try:
            logger.info(f"🔗 Connecting to LiveKit room: {self.room_name}")
            
            # Create room if it doesn't exist
            lkapi = api.LiveKitAPI(LIVEKIT_URL, LIVEKIT_API_KEY, LIVEKIT_API_SECRET)
            try:
                await lkapi.room.create_room(api.CreateRoomRequest(name=self.room_name))
                logger.info(f"✅ Created LiveKit room: {self.room_name}")
            except Exception as e:
                logger.info(f"ℹ️ Room creation result (may already exist): {e}")
            
            # Create access token
            token = (api.AccessToken(LIVEKIT_API_KEY, LIVEKIT_API_SECRET)
                    .with_identity(identity)
                    .with_name(PARTICIPANT_NAME)
                    .with_grants(api.VideoGrants(
                        room_join=True,
                        room=self.room_name,
                    ))) 
            
            # Connect to room
            self.room = rtc.Room()
            
            # Setup event handlers
            self._setup_room_events()

            # Connect to room with timeout
            logger.info(f"⏰ Connecting to LiveKit room with 10s timeout...")
            await asyncio.wait_for(
                self.room.connect(LIVEKIT_URL, token.to_jwt()), 
                timeout=10.0
            )
            logger.info(f"✅ LiveKit room connection successful!")
            
            # Mark as connected immediately
            self.connected = True
            
            # Create and publish audio track
            self.audio_source = TelephonyAudioSource()
            self.audio_track = rtc.LocalAudioTrack.create_audio_track(
                "telephony-audio", 
                self.audio_source
            )
            
            # Publish with microphone source
            options = rtc.TrackPublishOptions()
            options.source = rtc.TrackSource.SOURCE_MICROPHONE
            
            publication = await self.room.local_participant.publish_track(
                self.audio_track,
                options
            )
            logger.info(f"✅ Telephony audio track published: {publication.sid}")
            logger.info(f"🎯 LiveKit connection complete - ready for audio!")
            
            await lkapi.aclose()
            return True
            
        except asyncio.TimeoutError:
            logger.error(f"❌ LiveKit connection timeout after 10 seconds")
            return False
        except Exception as e:
            logger.error(f"❌ Failed to connect to LiveKit: {e}")
            import traceback
            traceback.print_exc()
            return False

    def _setup_room_events(self):
        """Setup LiveKit room event handlers"""
        
        @self.room.on("connected")
        def on_connected():
            logger.info(f"✅ LiveKit connection established for room: {self.room_name}")
            logger.info(f"�� Current participants in room: {len(self.room.remote_participants)}")
            self.connected = True
            
            # Check for existing participants
            for participant in self.room.remote_participants.values():
                logger.info(f"🔍 Found existing participant: {participant.identity}")
                self._handle_participant_joined(participant)

        @self.room.on("disconnected")
        def on_disconnected():
            logger.info(f"❌ LiveKit connection lost for room: {self.room_name}")
            self.connected = False

        @self.room.on("participant_connected")
        def on_participant_connected(participant):
            logger.info(f"👤 NEW participant joined: {participant.identity}")
            self._handle_participant_joined(participant)

        @self.room.on("participant_disconnected")
        def on_participant_disconnected(participant):
            logger.info(f"👋 Participant left: {participant.identity}")
            
            # Clean up tracking
            if participant.identity in self.participants:
                del self.participants[participant.identity]
            if participant.identity in self.audio_tracks:
                del self.audio_tracks[participant.identity]
            
            if participant == self.agent_participant:
                logger.warning("🤖 AGENT PARTICIPANT DISCONNECTED!")
                self.agent_participant = None
                # Cancel audio streaming task
                if self.audio_stream_task and not self.audio_stream_task.done():
                    logger.info("🔄 Cancelling audio stream task due to agent disconnect")
                    self.audio_stream_task.cancel()

        @self.room.on("track_published")
        def on_track_published(publication, participant):
            logger.info(f"📡 Track PUBLISHED by {participant.identity}: {publication.kind}")
            
            if self._is_agent_participant(participant):
                logger.info(f"🤖 AGENT published {publication.kind} track")

        @self.room.on("track_subscribed")
        def on_track_subscribed(track, publication, participant):
            logger.info(f"🎵 Track SUBSCRIBED from {participant.identity}: {track.kind}")
            
            # Store the track
            if participant.identity not in self.audio_tracks:
                self.audio_tracks[participant.identity] = []
            
            if track.kind == rtc.TrackKind.KIND_AUDIO:
                self.audio_tracks[participant.identity].append(track)
                logger.info(f"🔊 AUDIO TRACK STORED for {participant.identity}")
                
                # Check if this participant is the agent
                if self._is_agent_participant(participant):
                    logger.info(f"🤖 AGENT AUDIO TRACK CONFIRMED! Starting stream to telephony...")
                    self._start_agent_audio_stream(participant, track)

        @self.room.on("track_unsubscribed")
        def on_track_unsubscribed(track, publication, participant):
            logger.info(f"🔇 Track unsubscribed from {participant.identity}: {track.kind}")
            
            # Remove from tracking
            if participant.identity in self.audio_tracks:
                if track in self.audio_tracks[participant.identity]:
                    self.audio_tracks[participant.identity].remove(track)

    def _handle_participant_joined(self, participant):
        """Handle when a participant joins"""
        logger.info(f"🔍 Analyzing participant: {participant.identity}")
        
        # Store participant
        self.participants[participant.identity] = participant
        
        # Check if this is an agent
        is_agent, reasons = self._is_agent_participant(participant, return_reasons=True)
        
        if is_agent:
            self.agent_participant = participant
            logger.info(f"🤖 AGENT DETECTED: {participant.identity}")
            logger.info(f"🤖 Detection reasons: {', '.join(reasons)}")
            
            # Check if agent already has published audio tracks
            self._check_existing_agent_tracks(participant)
        else:
            logger.info(f"👥 Regular participant: {participant.identity}")

    def _is_agent_participant(self, participant, return_reasons=False):
        """Check if participant is an agent"""
        reasons = []
        is_agent = False
        
        identity = participant.identity.lower()
        
        # Check various agent patterns
        agent_patterns = [
            ("agent-", "identity starts with 'agent-'"),
            (agent_name, "identity contains 'outbound-caller'"),
            ("ac_", "identity starts with 'AC_'"),
            ("agent", "identity contains 'agent'"),
            ("assistant", "identity contains 'assistant'"),
            ("ai-", "identity starts with 'ai-'"),
        ]
        
        for pattern, reason in agent_patterns:
            if pattern in identity:
                is_agent = True
                reasons.append(reason)
        
        if return_reasons:
            return is_agent, reasons
        return is_agent

    def _check_existing_agent_tracks(self, participant):
        """Check if agent already has published tracks"""
        logger.info(f"🔍 Checking existing tracks for agent: {participant.identity}")
        
        track_publications = list(participant.track_publications.values())
        logger.info(f"🔍 Agent has {len(track_publications)} published tracks")
        
        for publication in track_publications:
            if publication.kind == rtc.TrackKind.KIND_AUDIO and publication.subscribed and publication.track:
                logger.info(f"🤖 Found existing AGENT AUDIO TRACK! Starting stream...")
                self._start_agent_audio_stream(participant, publication.track)

    def _start_agent_audio_stream(self, participant, track):
        """Start streaming agent audio to telephony"""
        # Cancel existing stream task if any
        if self.audio_stream_task and not self.audio_stream_task.done():
            logger.info("🔄 Cancelling existing audio stream task")
            self.audio_stream_task.cancel()
        
        # Start new audio streaming task
        logger.info("🚀 Creating new audio stream task")
        self.audio_stream_task = asyncio.create_task(
            self.stream_agent_audio_to_telephony(track, participant.identity)
        )

    async def stream_agent_audio_to_telephony(self, audio_track, participant_identity):
        """Stream agent's audio back to telephony system"""
        logger.info(f"🔊 Starting agent audio stream to telephony from {participant_identity}")
        
        frame_count = 0
        last_log_time = time.time()
        bytes_sent = 0
        
        try:
            # Create audio stream
            audio_stream = rtc.AudioStream(audio_track)
            logger.info("✅ AudioStream created successfully")
            
            async for audio_frame_event in audio_stream:
                current_time = time.time()
                
                # Check if still connected - fix the websocket check
                try:
                    websocket_closed = self.websocket.closed if hasattr(self.websocket, 'closed') else False
                except:
                    websocket_closed = False
                
                if not self.connected or websocket_closed or not self.call_active:
                    logger.warning("❌ Connection lost or call ended, stopping audio stream")
                    break
                
                frame_count += 1
                
                # Log every second
                if current_time - last_log_time >= 1.0:
                    logger.info(f"🔊 [OUTGOING] Agent audio: {frame_count} frames, {bytes_sent} bytes sent")
                    last_log_time = current_time
                
                try:
                    # Get the audio frame
                    frame = audio_frame_event.frame
                    
                    # Resample from 48kHz to 8kHz for telephony
                    resampled_frames = self.return_resampler.push(frame)
                    
                    for resampled_frame in resampled_frames:
                        # Convert to PCM bytes
                        pcm_bytes = bytes(resampled_frame.data[:resampled_frame.samples_per_channel * 2])
                        
                        # ADD NOISE HERE - before μ-law conversion
                        pcm_array = np.frombuffer(pcm_bytes, dtype=np.int16)
                        noise = np.random.normal(0, 0.02, len(pcm_array))
                        noisy_pcm = (pcm_array + noise * 32767 * 0.1).astype(np.int16)

                        # Convert PCM to μ-law for telephony
                        mulaw_bytes = audioop.lin2ulaw(pcm_bytes, 2)
                        
                        # Send to telephony
                        success = await self.send_audio_to_telephony(mulaw_bytes)
                        
                        if success:
                            bytes_sent += len(mulaw_bytes)
                            self.stats["audio_frames_received_from_agent"] += 1
                            self.stats["bytes_to_telephony"] += len(mulaw_bytes)
                        
                except Exception as e:
                    logger.error(f"❌ Error processing audio frame {frame_count}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"❌ Error in agent audio stream: {e}")
            import traceback
            traceback.print_exc()
        finally:
            logger.info(f"🔇 Agent audio stream ended. Frames: {frame_count}, Bytes: {bytes_sent}")

    async def send_audio_to_telephony(self, audio_data):
        """Send audio data back to Plivo via WebSocket"""
        try:
            # Check WebSocket connection status properly
            try:
                # For websockets library, check if connection is open
                websocket_closed = (not hasattr(self.websocket, 'open') or 
                                  not self.websocket.open if hasattr(self.websocket, 'open') else
                                  getattr(self.websocket, 'closed', False))
            except:
                # If we can't determine status, assume closed
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
            
            await self.websocket.send(json.dumps(media_message))
            self.messages_sent += 1
            
            # Log success for first few messages
            if self.messages_sent <= 5:
                logger.info(f"📤 SUCCESS: Sent agent audio #{self.messages_sent} to Plivo ({len(audio_data)} bytes)")
            # Log occasionally for subsequent messages
            elif self.messages_sent % 50 == 0:
                logger.info(f"📤 Sent {self.messages_sent} audio messages to Plivo")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error sending audio to Plivo: {e}")
            return False
        
    async def handle_messages(self):
        """Handle incoming WebSocket messages from Plivo"""
        logger.info(f"👂 Starting to listen for Plivo WebSocket messages...")
        last_log_time = time.time()
        
        try:
            async for message in self.websocket:
                self.messages_received += 1
                current_time = time.time()
                
                # Log raw message info with first 10 chars for debugging
                if isinstance(message, str):
                    # Log first 10 messages completely to catch start event
                    if self.messages_received <= 10:  
                        logger.info(f"📄 FULL MSG #{self.messages_received}: {message}")
                    # Then log every 5 seconds
                    elif current_time - last_log_time >= 5.0:
                        logger.info(f"📥 [MSG #{self.messages_received}] Processing {len(message)} char messages...")
                        last_log_time = current_time
                else:
                    # First few binary messages
                    if self.messages_received <= 10:
                        logger.info(f"📄 FULL BINARY #{self.messages_received}: {message[:100]}...")
                    # Then log every 5 seconds  
                    elif current_time - last_log_time >= 5.0:
                        logger.info(f"📥 [MSG #{self.messages_received}] Processing binary messages...")
                        last_log_time = current_time
                
                try:
                    if isinstance(message, str):
                        event = json.loads(message)
                        await self.handle_telephony_event(event)
                    else:
                        # Handle binary audio data directly
                        await self.handle_binary_audio(message)
                        
                except json.JSONDecodeError as e:
                    logger.error(f"❌ Invalid JSON from Plivo: {e}")
                    logger.error(f"Message content: {message[:100]}...")
                        
        except websockets.ConnectionClosed:
            logger.info("📞 Plivo WebSocket connection closed normally")
        except Exception as e:
            logger.error(f"❌ Error handling Plivo messages: {e}")
            import traceback
            traceback.print_exc()
        finally:
            await self.cleanup()
            
    async def handle_telephony_event(self, event):
        """Handle Plivo WebSocket events"""
        event_type = event.get("event")
        
        if event_type == "start":
            logger.info("🟢 CALL STARTED")
            logger.info(f"📞 Plivo event: {event_type}")
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
                
        elif event_type == "media":
            # Only log Plivo event for first few or every 5 seconds
            if self.messages_received <= 10:
                logger.info(f"📞 Plivo event: {event_type}")
            
            # Handle base64 encoded audio from Plivo (μ-law format)
            media_data = event.get("media", {})
            payload = media_data.get("payload")
            track = media_data.get("track", "inbound")
            
            if payload and self.audio_source:
                if not self.connected:
                    # Log connection issue but continue trying to connect
                    if self.messages_received % 250 == 0:  # Much less frequent
                        logger.warning(f"⚠️ LiveKit not connected yet (msg #{self.messages_received})")
                
                try:
                    # Decode base64 audio data (μ-law format from Plivo)
                    decoded_audio = base64.b64decode(payload)
                    
                    # Only process if we have a connected audio source
                    if self.connected:
                        await self.audio_source.push_audio_data(decoded_audio)
                        self.stats["audio_frames_sent_to_livekit"] += 1
                        self.stats["bytes_from_telephony"] += len(decoded_audio)
                        
                        # Log much less frequently
                        if self.stats["audio_frames_sent_to_livekit"] % 250 == 0:
                            logger.info(f"🎵 Processed {self.stats['audio_frames_sent_to_livekit']} audio frames from Plivo")
                    else:
                        # Count dropped frames
                        if not hasattr(self, 'dropped_frames'):
                            self.dropped_frames = 0
                        self.dropped_frames += 1
                        
                except Exception as e:
                    logger.error(f"❌ Error processing Plivo media: {e}")
            elif not payload:
                if self.messages_received <= 10:
                    logger.warning("⚠️ Media event without payload")
            elif not self.audio_source:
                if self.messages_received <= 10:
                    logger.warning("⚠️ Audio source not initialized")
                    
        elif event_type == "stop":
            logger.info("🔴 CALL ENDED")
            logger.info(f"📞 Plivo event: {event_type}")
            self.call_active = False
            await self.cleanup()
            
        else:
            logger.info(f"❓ Unknown Plivo event: {event_type}")
            logger.info(f"�� Event data: {json.dumps(event, indent=2)}")

    async def handle_binary_audio(self, audio_data):
        """Handle binary audio data directly"""
        if self.audio_source and self.connected:
            try:
                await self.audio_source.push_audio_data(audio_data)
                self.stats["audio_frames_sent_to_livekit"] += 1
                self.stats["bytes_from_telephony"] += len(audio_data)
            except Exception as e:
                logger.error(f"❌ Error processing binary audio: {e}")
            
    async def cleanup(self):
        """Clean up resources"""
        logger.info("🧹 Starting cleanup...")
        
        self.call_active = False
        
        # Remove from global tracking
        try:
            import __main__
            if hasattr(__main__, 'active_handlers') and self in __main__.active_handlers:
                __main__.active_handlers.remove(self)
        except:
            pass
        
        # Cancel audio streaming task
        if self.audio_stream_task and not self.audio_stream_task.done():
            logger.info("🔄 Cancelling audio stream task...")
            self.audio_stream_task.cancel()
            try:
                await asyncio.wait_for(self.audio_stream_task, timeout=2.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
        
        # Cleanup audio source
        if self.audio_source:
            try:
                await self.audio_source.cleanup()
            except Exception as e:
                logger.error(f"❌ Error cleaning up audio source: {e}")
            
        # Disconnect from LiveKit room
        if self.room and self.connected:
            try:
                self.connected = False
                await asyncio.wait_for(self.room.disconnect(), timeout=3.0)
                logger.info("✅ Disconnected from LiveKit room")
            except asyncio.TimeoutError:
                logger.warning("⚠️ LiveKit disconnect timeout")
            except Exception as e:
                logger.error(f"❌ Error disconnecting from LiveKit: {e}")
        
        # Log final statistics
        elapsed = time.time() - self.connection_start_time
        dropped_frames = getattr(self, 'dropped_frames', 0)
        
        logger.info(f"📊 Session Summary:")
        logger.info(f"   Duration: {elapsed:.1f}s")
        logger.info(f"   Messages: {self.messages_received} received, {self.messages_sent} sent")
        logger.info(f"   Audio to LiveKit: {self.stats['audio_frames_sent_to_livekit']} frames, {self.stats['bytes_from_telephony']} bytes")
        logger.info(f"   Audio from Agent: {self.stats['audio_frames_received_from_agent']} frames, {self.stats['bytes_to_telephony']} bytes")
        logger.info(f"   Dropped frames (no LiveKit): {dropped_frames}")
        logger.info(f"   Agent: {'Found' if self.agent_participant else 'Not found'}")
        
        # Clean up room after call ends - try to end the room nicely
        if self.room_name:
            logger.info(f"🧹 Notifying room cleanup: {self.room_name}")
            try:
                # Create API instance to clean up room
                lkapi = api.LiveKitAPI(LIVEKIT_URL, LIVEKIT_API_KEY, LIVEKIT_API_SECRET)
                
                # List participants to see if room is empty
                try:
                    participants = await lkapi.room.list_participants(api.ListParticipantsRequest(room=self.room_name))
                    logger.info(f"📊 Room {self.room_name} has {len(participants.participants)} participants remaining")
                    
                    # If only the agent is left, we could disconnect it
                    if len(participants.participants) == 1:
                        remaining = participants.participants[0]
                        if self._is_agent_participant_identity(remaining.identity):
                            logger.info(f"🤖 Only agent left in room, considering cleanup...")
                            # Let the agent finish gracefully - it should disconnect when it detects no human participants
                            
                except Exception as e:
                    logger.error(f"❌ Error checking room participants: {e}")
                
                await lkapi.aclose()
                
            except Exception as e:
                logger.error(f"❌ Error during room cleanup: {e}")
            
        logger.info("✅ Handler cleanup complete")
    
    def _is_agent_participant_identity(self, identity: str) -> bool:
        """Check if identity string belongs to an agent"""
        identity = identity.lower()
        agent_patterns = ["agent-", "outbound-caller", "ac_", "assistant", "ai-"]
        return any(pattern in identity for pattern in agent_patterns)

# async def trigger_agent(room_name: str):
#     """Launch agent in the specified LiveKit room"""
#     logger.info(f"�� Triggering agent for room: {room_name}")
#     try:
#         # Wait for the room to be ready and participant to connect
#         logger.info(f"⏰ Waiting 2 seconds for room setup...")
#         await asyncio.sleep(2)
        
#         logger.info(f"🎯 Dispatching agent to room: {room_name}")
#         result = subprocess.Popen([
#             "lk", "dispatch", "create",
#             "--room", room_name,
#             "--agent-name", agent_name  # Change this to your agent name
#         ])
        
#         logger.info(f"✅ Agent dispatch command executed (PID: {result.pid})")
        
#     except Exception as e:
#         logger.error(f"❌ Error launching agent: {e}")

#New Changes
async def trigger_agent(room_name: str):
    """Launch agent in the specified LiveKit room - OPTIMIZED VERSION"""
    logger.info(f"🚀 Triggering agent for room: {room_name}")
    try:
        # NO DELAY - start agent immediately
        logger.info(f"�� Dispatching agent to room: {room_name}")
        
        # Use Popen for non-blocking execution
        result = subprocess.Popen([
            "lk", "dispatch", "create",
            "--room", room_name,
            "--agent-name", agent_name
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        logger.info(f"✅ Agent dispatch command executed immediately (PID: {result.pid})")
        
        # Optional: Check if command started successfully
        # Give it a moment to start, but don't wait for completion
        await asyncio.sleep(0.1)  # Just 100ms to let process start
        
        if result.poll() is None:  # Process is still running
            logger.info(f"✅ Agent process started successfully")
        else:
            # Process ended quickly, check for errors
            stdout, stderr = result.communicate()
            if result.returncode != 0:
                logger.error(f"❌ Agent dispatch failed: {stderr.decode()}")
            else:
                logger.info(f"✅ Agent dispatch completed: {stdout.decode()}")
        
    except Exception as e:
        logger.error(f"❌ Error launching agent: {e}")

async def handle_telephony_websocket(websocket, path):
    """Handle incoming WebSocket connections from Plivo - OPTIMIZED"""
    try:
        logger.info(f"🔗 NEW PLIVO WEBSOCKET CONNECTION")
        logger.info(f"📍 Path: {path}")
        
        # Parse room name from query parameters
        parsed_url = urlparse(path)
        query = parse_qs(parsed_url.query)
        room_name = query.get("room", [f"plivo-room-{uuid.uuid4()}"])[0]
        
        logger.info(f"📞 Room: {room_name}")
        
        # Create handler for Plivo WebSocket
        handler = TelephonyWebSocketHandler(room_name, websocket)
        
        # OPTIMIZATION: Start all tasks concurrently
        logger.info(f"🚀 Starting concurrent setup...")
        
        # Start all three tasks at the same time
        livekit_task = asyncio.create_task(handler.connect_to_livekit())
        agent_task = asyncio.create_task(trigger_agent(room_name))
        message_task = asyncio.create_task(handler.handle_messages())
        
        # Wait for LiveKit connection with shorter timeout
        try:
            success = await asyncio.wait_for(livekit_task, timeout=8.0)  # Reduced from 15s
            if success:
                logger.info(f"✅ LiveKit connected for room: {room_name}")
            else:
                logger.error("❌ LiveKit connection failed")
        except asyncio.TimeoutError:
            logger.error("❌ LiveKit connection timeout (8s)")
        
        # Agent task should complete quickly (just process start)
        try:
            await asyncio.wait_for(agent_task, timeout=2.0)
            logger.info("✅ Agent dispatch completed")
        except asyncio.TimeoutError:
            logger.warning("⚠️ Agent dispatch took longer than expected")
        
        # Wait for message handling to complete
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
#New Changes



# async def handle_telephony_websocket(websocket, path):
#     """Handle incoming WebSocket connections from Plivo"""
#     try:
#         logger.info(f"�� NEW PLIVO WEBSOCKET CONNECTION")
#         logger.info(f"📍 Path: {path}")
#         logger.info(f"🌐 Remote address: {websocket.remote_address}")
        
#         # Parse room name from query parameters
#         parsed_url = urlparse(path)
#         query = parse_qs(parsed_url.query)
#         room_name = query.get("room", [f"plivo-room-{uuid.uuid4()}"])[0]
        
#         logger.info(f"�� Room: {room_name}")
        
#         # Create handler for Plivo WebSocket
#         handler = TelephonyWebSocketHandler(room_name, websocket)
        
#         # Start LiveKit connection in background while handling messages
#         logger.info(f"🚀 Starting LiveKit connection in background...")
#         livekit_task = asyncio.create_task(handler.connect_to_livekit())
        
#         # Start handling messages immediately (to catch the start event)
#         logger.info(f"👂 Starting message handling...")
#         message_task = asyncio.create_task(handler.handle_messages())
        
#         # Wait for LiveKit connection
#         try:
#             success = await asyncio.wait_for(livekit_task, timeout=15.0)
#             if success:
#                 logger.info(f"✅ LiveKit connected, triggering agent for room: {room_name}")
#                 asyncio.create_task(trigger_agent(room_name))
#             else:
#                 logger.error("❌ LiveKit connection failed")
#         except asyncio.TimeoutError:
#             logger.error("❌ LiveKit connection timeout")
        
#         # Wait for message handling to complete
#         await message_task

#     except Exception as e:
#         logger.error(f"❌ Error in Plivo WebSocket handler: {e}")
#         import traceback
#         traceback.print_exc()
#         try:
#             if not websocket.closed:
#                 await websocket.close(code=1011, reason=str(e))
#         except:
#             pass

async def start_websocket_server():
    """Start the WebSocket server for Plivo connections"""
    logger.info("🌐 WebSocket server starting on ws://0.0.0.0:8765")
    
    async def websocket_handler(websocket):
        try:
            path = websocket.path if hasattr(websocket, 'path') else "/"
            await handle_telephony_websocket(websocket, path)
        except Exception as e:
            logger.error(f"❌ Error in websocket handler: {e}")
    
    # Start the server
    async with websockets.serve(websocket_handler, "0.0.0.0", 8765):
        logger.info("✅ WebSocket server listening on ws://0.0.0.0:8765")
        logger.info("🔧 Ready for Plivo WebSocket connections")
        logger.info("📋 Plivo should connect to: ws://sbi.vaaniresearch.com:8765/?room=your_room_name")
        await asyncio.Future()

async def start_http_server():
    """Start HTTP server for API endpoints"""
    
    async def handle_health(request):
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

    async def handle_trigger_room(request):
        """Trigger agent in a specific room"""
        try:
            data = await request.json()
            room = data["room"]
            
            logger.info(f"🎯 Manual agent trigger for room: {room}")
            asyncio.create_task(trigger_agent(room))
            
            return web.json_response({
                "status": "triggered",
                "room": room,
                "message": f"Agent triggered for room {room}"
            })
        except Exception as e:
            logger.error(f"❌ Error triggering agent: {e}")
            return web.json_response({"error": str(e)}, status=400)

    async def handle_plivo_xml(request):
        """Return Plivo XML for call flow - /plivo-app/plivo.xml"""
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
            return web.Response(text="<?xml version='1.0' encoding='UTF-8'?><Response><Hangup/></Response>", 
                              content_type="text/xml", status=500)

    async def handle_plivo_hangup(request):
        """Handle Plivo hangup callback - /plivo-app/hangup"""
        try:
            # Log the hangup event
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
            
            # Here you can add custom logic for call cleanup if needed
            # For example, notify other services, update databases, etc.
            
            return web.Response(text="OK", status=200)
            
        except Exception as e:
            logger.error(f"❌ Error processing hangup callback: {e}")
            return web.Response(text="Error", status=500)

    async def handle_stream_status(request):
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
            
            logger.info(f"�� STREAM STATUS - Stream: {stream_id}")
            logger.info(f"   Call: {call_uuid}")
            logger.info(f"   Status: {status}")
            logger.info(f"   Full data: {data}")
            
            return web.Response(text="OK", status=200)
            
        except Exception as e:
            logger.error(f"❌ Error processing stream status: {e}")
            return web.Response(text="Error", status=500)

    async def handle_trigger_call(request):
        """Trigger a new call via Plivo API - for testing purposes"""
        try:
            data = await request.json()
            to_number = data["to"]
            from_number = data["from"] 
            room = data.get("room", f"plivo-room-{uuid.uuid4()}")
            
            logger.info(f"📞 Triggering Plivo call: {from_number} -> {to_number} (room: {room})")
            
            # This would require Plivo credentials - implement if needed
            # result = initiate_plivo_call(to_number, from_number, room)
            
            return web.json_response({
                "status": "not_implemented",
                "room": room,
                "message": f"Call triggering not implemented - add Plivo credentials and uncomment code"
            })
        except Exception as e:
            logger.error(f"❌ Error triggering call: {e}")
            return web.json_response({"error": str(e)}, status=400)

    # Create web application
    app = web.Application()
    
    # Health and utility endpoints
    app.router.add_get("/health", handle_health)
    app.router.add_post("/trigger", handle_trigger_room)
    
    # Plivo-specific endpoints
    app.router.add_get("/plivo-app/plivo.xml", handle_plivo_xml)
    app.router.add_post("/plivo-app/hangup", handle_plivo_hangup)
    app.router.add_get("/plivo-app/hangup", handle_plivo_hangup)
    app.router.add_post("/plivo-app/stream-status", handle_stream_status)
    app.router.add_get("/plivo-app/stream-status", handle_stream_status)
    
    # Optional call triggering endpoint (requires Plivo credentials)
    app.router.add_post("/plivo-app/trigger-call", handle_trigger_call)
    
    # Start server
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8080)
    await site.start()
    logger.info("🌐 HTTP server listening on http://0.0.0.0:8080")
    logger.info("📋 Plivo XML endpoint: http://0.0.0.0:8080/plivo-app/plivo.xml")
    logger.info("📞 Plivo hangup callback: http://0.0.0.0:8080/plivo-app/hangup")

async def main():
    """Main function to run both servers"""
    logger.info("🚀 Starting Telephony-LiveKit Bridge...")
    logger.info("=" * 60)
    
    # Validate environment variables
    required_vars = ["LIVEKIT_URL", "LIVEKIT_API_KEY", "LIVEKIT_API_SECRET"]
    
    logger.info("🔧 Checking environment variables...")
    missing_vars = [var for var in required_vars if not os.environ.get(var)]
    if missing_vars:
        logger.error(f"❌ Missing required environment variables: {missing_vars}")
        return
    
    # Log configuration (without secrets)
    logger.info("✅ All environment variables configured")
    logger.info(f"🔗 LiveKit URL: {LIVEKIT_URL}")
    logger.info(f"📞 WebSocket URL: {CALLBACK_WS_URL}")
    logger.info(f"🎵 Audio Config: Telephony({TELEPHONY_SAMPLE_RATE}Hz) <-> LiveKit({LIVEKIT_SAMPLE_RATE}Hz)")
    logger.info("=" * 60)
    
    # Global cleanup tracking
    active_handlers = []
    
    async def cleanup_all_handlers():
        """Clean up all active handlers"""
        logger.info(f"🧹 Cleaning up {len(active_handlers)} active handlers...")
        cleanup_tasks = []
        for handler in active_handlers.copy():
            if hasattr(handler, 'cleanup'):
                cleanup_tasks.append(handler.cleanup())
        
        if cleanup_tasks:
            await asyncio.gather(*cleanup_tasks, return_exceptions=True)
        active_handlers.clear()
        logger.info("✅ All handlers cleaned up")
    
    # Store cleanup function globally for handlers to register themselves
    import __main__
    __main__.active_handlers = active_handlers
    __main__.cleanup_all_handlers = cleanup_all_handlers
    
    try:
        # Run both servers concurrently
        logger.info("🚀 Starting servers...")
        await asyncio.gather(
            start_websocket_server(),
            start_http_server()
        )
    except KeyboardInterrupt:
        logger.info("👋 Received shutdown signal")
        await cleanup_all_handlers()
    except Exception as e:
        logger.error(f"❌ Server error: {e}")
        import traceback
        traceback.print_exc()
        await cleanup_all_handlers()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Shutting down...")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()

