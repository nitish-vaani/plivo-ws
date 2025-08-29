import asyncio
import websockets
import websockets.protocol
import json
import logging
import uuid
import os
import base64
from livekit import rtc, api
import subprocess
import time
import audioop
from aiohttp import web
from collections import defaultdict, deque
import threading
from concurrent.futures import ThreadPoolExecutor
import wave
import struct
import array


# Environment variables
LIVEKIT_URL = os.environ.get("LIVEKIT_URL", "wss://setupforretell-hk7yl5xf.livekit.cloud")
LIVEKIT_API_KEY = os.environ.get("LIVEKIT_API_KEY", "APIoLr2sRCRJWY5")
LIVEKIT_API_SECRET = os.environ.get("LIVEKIT_API_SECRET", "yE3wUkoQxjWjhteMAed9ubm5mYg3iOfPT6qBQfffzgJC")
PARTICIPANT_NAME = "Maqsam Caller"
TELEPHONY_SAMPLE_RATE = 8000
LIVEKIT_SAMPLE_RATE = 48000
agent_name = "Mysyara Agent"

# Background audio settings
BACKGROUND_AUDIO_FILE = "call-center.mp3"
BACKGROUND_VOLUME_RATIO = 0.15
ENABLE_BACKGROUND_AUDIO = False

# Maqsam Authentication
VALID_AUTH_TOKEN = os.environ.get("MAQSAM_AUTH_TOKEN", "maqsam_secure_token_123")

# Optimized settings (reduced complexity)
AUDIO_FRAME_SIZE = 160  # 20ms at 8kHz - more standard than 10ms
MAX_BUFFER_SIZE = 2     # Slightly more buffering for stability
PROCESS_POOL_SIZE = 2   # Reduced thread pool size
ENABLE_AUDIO_OPTIMIZATION = True

# Connection timeouts and limits
AGENT_CONNECTION_TIMEOUT = 15  # Reduced from 30s
MAX_CONNECTIONS = 500
RATE_LIMIT_PER_IP = 10
RATE_LIMIT_WINDOW = 60

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global thread pool and background audio manager
audio_processor_pool = ThreadPoolExecutor(max_workers=PROCESS_POOL_SIZE)
global_background_audio_manager = None

# Connection tracking
active_connections = 0
connections_per_ip = defaultdict(int)
connection_attempts = defaultdict(lambda: deque())

def validate_auth_token(token: str) -> bool:
    """Validate Maqsam authentication token"""
    return token and token == VALID_AUTH_TOKEN

def create_room_from_context(context: dict) -> str:
    """Create room ID from Maqsam context"""
    try:
        if not isinstance(context, dict):
            return f"maqsam_fallback_{uuid.uuid4()}"
        
        call_id = context.get('id', uuid.uuid4().hex[:8])
        caller_number = context.get('caller_number', 'unknown')
        direction = context.get('direction', 'unknown')
        timestamp = context.get('timestamp', time.strftime('%Y%m%d_%H%M%S'))
        
        clean_timestamp = str(timestamp).replace(' ', '_').replace('-', '').replace(':', '')
        room_id = f"maqsam_{direction}_{caller_number}_{call_id}_{clean_timestamp}"
        logger.info(f"Created room ID: {room_id}")
        return room_id
        
    except Exception as e:
        logger.error(f"Error creating room from context: {e}")
        return f"maqsam_fallback_{uuid.uuid4()}"

def process_mulaw_to_pcm(mulaw_data):
    """Process μ-law conversion"""
    try:
        return audioop.ulaw2lin(mulaw_data, 2)
    except Exception as e:
        logger.error(f"μ-law conversion error: {e}")
        return None

def process_pcm_to_mulaw(pcm_data):
    """Process PCM to μ-law conversion"""
    try:
        return audioop.lin2ulaw(pcm_data, 2)
    except Exception as e:
        logger.error(f"PCM to μ-law conversion error: {e}")
        return None

class BackgroundAudioManager:
    """Manages background audio for calls"""
    
    def __init__(self, audio_file_path):
        self.audio_file_path = audio_file_path
        self.background_audio_data = None
        self.current_position = 0
        self.is_running = False
        self.lock = threading.Lock()
        
        logger.info("Initializing background audio manager")
        self._load_background_audio()
    
    def _load_background_audio(self):
        """Load and convert background audio to telephony format"""
        try:
            if not os.path.exists(self.audio_file_path):
                logger.warning(f"Background audio file not found: {self.audio_file_path}")
                return
            
            logger.info(f"Loading background audio: {self.audio_file_path}")
            
            # Use ffmpeg to convert to telephony format
            import tempfile
            with tempfile.NamedTemporaryFile(suffix='.wav', delete=False) as temp_file:
                temp_path = temp_file.name
            
            try:
                result = subprocess.run([
                    'ffmpeg', '-i', self.audio_file_path,
                    '-ar', str(TELEPHONY_SAMPLE_RATE),
                    '-ac', '1',
                    '-f', 'wav',
                    '-y',
                    temp_path
                ], capture_output=True, text=True, timeout=30)
                
                if result.returncode != 0:
                    logger.error(f"FFmpeg conversion failed: {result.stderr}")
                    return
                
                with wave.open(temp_path, 'rb') as wav_file:
                    if wav_file.getnchannels() != 1 or wav_file.getframerate() != TELEPHONY_SAMPLE_RATE:
                        logger.error("Background audio must be mono and 8kHz")
                        return
                    
                    pcm_data = wav_file.readframes(wav_file.getnframes())
                    self.background_audio_data = audioop.lin2ulaw(pcm_data, 2)
                    
                    duration = len(pcm_data) / (2 * TELEPHONY_SAMPLE_RATE)
                    logger.info(f"Background audio loaded: {len(self.background_audio_data)} bytes, {duration:.1f}s")
                
            finally:
                try:
                    os.unlink(temp_path)
                except:
                    pass
                    
        except Exception as e:
            logger.error(f"Error loading background audio: {e}")
    
    def get_audio_chunk(self, chunk_size):
        """Get a chunk of background audio data"""
        if not self.background_audio_data:
            return None
        
        with self.lock:
            data_len = len(self.background_audio_data)
            
            if self.current_position >= data_len:
                self.current_position = 0
            
            if self.current_position + chunk_size <= data_len:
                chunk = self.background_audio_data[self.current_position:self.current_position + chunk_size]
                self.current_position += chunk_size
            else:
                first_part = self.background_audio_data[self.current_position:]
                remaining = chunk_size - len(first_part)
                second_part = self.background_audio_data[:remaining]
                chunk = first_part + second_part
                self.current_position = remaining
            
            return chunk
    
    def start(self):
        self.is_running = True
        logger.info("Background audio started")
    
    def stop(self):
        self.is_running = False
        logger.info("Background audio stopped")

class OptimizedAudioBuffer:
    """Audio buffer with minimal latency"""
    
    def __init__(self, max_size=MAX_BUFFER_SIZE):
        self.buffer = deque(maxlen=max_size)
        self.lock = threading.Lock()
        self.dropped_frames = 0
    
    def push(self, data):
        with self.lock:
            if len(self.buffer) >= self.buffer.maxlen:
                self.buffer.popleft()
                self.dropped_frames += 1
            self.buffer.append(data)
    
    def pop_all(self):
        with self.lock:
            data = list(self.buffer)
            self.buffer.clear()
            return data

class OptimizedMaqsamAudioSource(rtc.AudioSource):
    """Optimized audio source for Maqsam"""
    
    def __init__(self):
        super().__init__(
            sample_rate=LIVEKIT_SAMPLE_RATE,
            num_channels=1
        )
        
        self.resampler = rtc.AudioResampler(
            input_rate=TELEPHONY_SAMPLE_RATE,
            output_rate=LIVEKIT_SAMPLE_RATE,
            num_channels=1,
            quality=rtc.AudioResamplerQuality.LOW
        )
        
        self.audio_buffer = OptimizedAudioBuffer()
        self.processing_task = None
        self.should_process = True
        
        logger.info(f"Audio Source initialized: {TELEPHONY_SAMPLE_RATE}Hz -> {LIVEKIT_SAMPLE_RATE}Hz")

    async def start_processing(self):
        if not self.processing_task:
            self.processing_task = asyncio.create_task(self._process_audio_buffer())

    async def push_audio_data(self, mulaw_data):
        if not mulaw_data or not self.should_process:
            return

        self.audio_buffer.push(mulaw_data)
        
        if not self.processing_task:
            await self.start_processing()

    async def _process_audio_buffer(self):
        logger.info("Starting audio processing")
        
        while self.should_process:
            try:
                audio_chunks = self.audio_buffer.pop_all()
                
                if not audio_chunks:
                    await asyncio.sleep(0.005)  # 5ms sleep when no data
                    continue
                
                for mulaw_data in audio_chunks:
                    await self._process_single_chunk(mulaw_data)
                
            except Exception as e:
                logger.error(f"Error in audio processing loop: {e}")
                await asyncio.sleep(0.01)

    async def _process_single_chunk(self, mulaw_data):
        try:
            if ENABLE_AUDIO_OPTIMIZATION:
                loop = asyncio.get_event_loop()
                pcm_data = await loop.run_in_executor(
                    audio_processor_pool, 
                    process_mulaw_to_pcm, 
                    mulaw_data
                )
            else:
                pcm_data = audioop.ulaw2lin(mulaw_data, 2)
            
            if not pcm_data:
                return
            
            samples = array.array("h")
            samples.frombytes(pcm_data)

            if len(samples) == 0:
                return

            input_frame = rtc.AudioFrame.create(
                sample_rate=TELEPHONY_SAMPLE_RATE,
                num_channels=1,
                samples_per_channel=len(samples)
            )
            input_frame.data[:len(samples)] = samples

            resampled_frames = self.resampler.push(input_frame)
            for resampled_frame in resampled_frames:
                await self.capture_frame(resampled_frame)

        except Exception as e:
            logger.error(f"Error processing audio chunk: {e}")

    async def cleanup(self):
        try:
            self.should_process = False
            
            if self.processing_task:
                self.processing_task.cancel()
                try:
                    await asyncio.wait_for(self.processing_task, timeout=1.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
            
            logger.debug("Audio source cleanup complete")
            
        except Exception as e:
            logger.error(f"Error cleaning up audio source: {e}")

class CallTerminationError(Exception):
    """Exception raised when call should be terminated"""
    pass

class OptimizedMaqsamWebSocketHandler:
    """Optimized WebSocket handler with robust error handling"""
    
    def __init__(self, websocket):
        self.websocket = websocket
        self.authenticated = False
        self.session_ready = False
        self.call_active = True
        self.context = None
        self.room_name = None
        self.room = None
        self.audio_source = None
        self.audio_track = None
        self.connected_to_livekit = False
        self.agent_participant = None
        self.connection_start_time = time.time()
        self.messages_received = 0
        self.messages_sent = 0
        self.audio_stream_task = None
        self.background_stream_task = None
        self.agent_is_speaking = False
        self.agent_connection_timeout_task = None
        
        self.participants = {}
        self.audio_tracks = {}
        
        self.return_resampler = rtc.AudioResampler(
            input_rate=LIVEKIT_SAMPLE_RATE,
            output_rate=TELEPHONY_SAMPLE_RATE,
            num_channels=1,
            quality=rtc.AudioResamplerQuality.LOW
        )
        
        self.return_audio_buffer = OptimizedAudioBuffer()
        self.background_audio_manager = global_background_audio_manager
        
        self.stats = {
            "audio_frames_sent_to_livekit": 0,
            "audio_frames_received_from_agent": 0,
            "bytes_from_maqsam": 0,
            "bytes_to_maqsam": 0
        }
        
        logger.info("Created optimized WebSocket handler")
    
    def _is_websocket_open(self):
        """Check if websocket connection is open"""
        try:
            return self.websocket.state == websockets.protocol.State.OPEN
        except:
            return False
    
    async def handle_connection(self):
        """Main handler for Maqsam WebSocket connection"""
        try:
            # Start background audio immediately
            if self.background_audio_manager:
                self.background_audio_manager.start()
                self.background_stream_task = asyncio.create_task(self._stream_background_audio())
                logger.info("Background audio streaming started")
            
            # Handle messages
            async for message in self.websocket:
                self.messages_received += 1
                await self._process_message_async(message)
                
                if not self.call_active and self.session_ready:
                    logger.info("Call ended, breaking message loop")
                    break
                    
        except websockets.ConnectionClosed:
            logger.info("Connection closed normally")
        except CallTerminationError as e:
            logger.info(f"Call terminated: {e}")
        except Exception as e:
            logger.error(f"Error in handler: {e}")
        finally:
            await self.cleanup()
    
    async def _process_message_async(self, message):
        try:
            if isinstance(message, str):
                data = json.loads(message)
                await self._handle_json_message(data)
            elif isinstance(message, bytes):
                decoded = message.decode('utf-8')
                data = json.loads(decoded)
                await self._handle_json_message(data)
                    
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON from Maqsam: {e}")
        except Exception as e:
            logger.error(f"Error processing message: {e}")
    
    async def _handle_json_message(self, data):
        message_type = data.get("type")
        
        if message_type == "session.setup":
            await self._handle_session_setup(data)
        elif message_type == "audio.input":
            await self._handle_audio_input(data)
        elif message_type == "call.mark":
            await self._handle_call_mark(data)
        elif message_type == "call.dtmf":
            await self._handle_dtmf(data)
        else:
            logger.debug(f"Unknown message type: {message_type}")
    
    async def _handle_session_setup(self, data):
        """Handle session.setup with improved error handling"""
        logger.info("Handling session.setup")
        
        # Validate authentication
        api_key = data.get("apiKey")
        if api_key:
            if not validate_auth_token(api_key):
                logger.error("Invalid API key provided")
                await self.websocket.close(code=1008, reason="Invalid API key")
                self.call_active = False
                return
            self.authenticated = True
            logger.info("Authenticated successfully")
        
        # Extract context
        session_data = data.get("data", {})
        raw_context = session_data.get("context")
        
        try:
            if isinstance(raw_context, str):
                self.context = json.loads(raw_context)
            elif isinstance(raw_context, dict):
                self.context = raw_context
            else:
                self.context = {}
        except:
            self.context = {}
        
        self.room_name = create_room_from_context(self.context)
        
        # Send session ready
        await self._send_session_ready()
        
        # Connect to LiveKit
        success = await self._connect_to_livekit()
        if not success:
            logger.error("Failed to connect to LiveKit")
            raise CallTerminationError("LiveKit connection failed")
        
        # Start agent connection monitoring with timeout
        self.agent_connection_timeout_task = asyncio.create_task(
            self._monitor_agent_with_timeout()
        )

    async def _send_session_ready(self):
        ready_message = {"type": "session.ready"}
        await self.websocket.send(json.dumps(ready_message))
        self.session_ready = True
        self.messages_sent += 1
        logger.info("Sent session.ready")
    
    async def _handle_audio_input(self, data):
        if not self.session_ready:
            return
        
        audio_data = data.get("data", {})
        base64_audio = audio_data.get("audio")
        
        if base64_audio and self.audio_source:
            try:
                mulaw_data = base64.b64decode(base64_audio)
                
                if self.connected_to_livekit:
                    await self.audio_source.push_audio_data(mulaw_data)
                    self.stats["audio_frames_sent_to_livekit"] += 1
                    self.stats["bytes_from_maqsam"] += len(mulaw_data)
                
            except Exception as e:
                logger.error(f"Error processing audio: {e}")
    
    async def _handle_call_mark(self, data):
        mark_data = data.get("data", {})
        label = mark_data.get("label")
        logger.info(f"Received call.mark: {label}")
    
    async def _handle_dtmf(self, data):
        dtmf_data = data.get("data", {})
        digit = dtmf_data.get("digit")
        logger.info(f"Received DTMF digit: {digit}")
    
    async def _connect_to_livekit(self):
        """Connect to LiveKit with error handling"""
        if not self.room_name:
            logger.error("No room name available")
            return False
        
        calling_number = "unknown"
        if self.context and isinstance(self.context, dict):
            calling_number = self.context.get('caller_number', 'unknown')
        
        identity = f"caller-{calling_number}-{int(time.time())}"
        participant_name = f"Maqsam Caller {calling_number}"
        
        logger.info(f"Connecting as identity: {identity}")
        
        try:
            logger.info(f"Connecting to LiveKit room: {self.room_name}")
            
            lkapi = api.LiveKitAPI(LIVEKIT_URL, LIVEKIT_API_KEY, LIVEKIT_API_SECRET)
            
            token = (api.AccessToken(LIVEKIT_API_KEY, LIVEKIT_API_SECRET)
                    .with_identity(identity)
                    .with_name(participant_name)
                    .with_grants(api.VideoGrants(
                        room_join=True,
                        room=self.room_name,
                        can_publish=True,
                        can_subscribe=True,
                        can_publish_data=True,
                    )))
            
            # Create room
            try:
                await lkapi.room.create_room(api.CreateRoomRequest(name=self.room_name))
                logger.info(f"Created LiveKit room: {self.room_name}")
            except Exception as e:
                logger.debug(f"Room creation result: {e}")
            
            # Connect to room
            self.room = rtc.Room()
            self._setup_room_events()

            await asyncio.wait_for(
                self.room.connect(LIVEKIT_URL, token.to_jwt()), 
                timeout=10.0
            )
            logger.info("LiveKit room connection successful")
            
            self.connected_to_livekit = True
            
            # Setup audio track
            self.audio_source = OptimizedMaqsamAudioSource()
            await self.audio_source.start_processing()
            
            self.audio_track = rtc.LocalAudioTrack.create_audio_track(
                "maqsam-audio", 
                self.audio_source
            )
            
            options = rtc.TrackPublishOptions()
            options.source = rtc.TrackSource.SOURCE_MICROPHONE
            
            await self.room.local_participant.publish_track(self.audio_track, options)
            logger.info("Audio track published")
            
            # Trigger agent
            asyncio.create_task(self._trigger_agent())
            
            await lkapi.aclose()
            
            logger.info(f"Connected participants: {len(self.room.remote_participants)}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to LiveKit: {e}")
            return False
    
    def _setup_room_events(self):
        """Setup LiveKit room event handlers"""
        
        @self.room.on("connected")
        def on_connected():
            logger.info("LiveKit connected")
            self.connected_to_livekit = True

        @self.room.on("disconnected")
        def on_disconnected():
            logger.info("LiveKit connection lost")
            self.connected_to_livekit = False

        @self.room.on("participant_connected")
        def on_participant_connected(participant):
            logger.info(f"Participant joined: {participant.identity}")
            self._handle_participant_joined(participant)

        @self.room.on("participant_disconnected")
        def on_participant_disconnected(participant):
            logger.info(f"Participant left: {participant.identity}")
            
            # Clean up tracking
            if participant.identity in self.participants:
                del self.participants[participant.identity]
            if participant.identity in self.audio_tracks:
                del self.audio_tracks[participant.identity]
            
            # CRITICAL: If agent disconnects, terminate the call
            if participant == self.agent_participant:
                logger.warning("AGENT DISCONNECTED - TERMINATING CALL")
                self.agent_participant = None
                if self.audio_stream_task and not self.audio_stream_task.done():
                    self.audio_stream_task.cancel()
                
                # Terminate the call immediately
                asyncio.create_task(self._terminate_call_due_to_agent_disconnect())

        @self.room.on("track_subscribed")
        def on_track_subscribed(track, publication, participant):
            logger.info(f"Track subscribed from {participant.identity}: {track.kind}")
            
            if participant.identity not in self.audio_tracks:
                self.audio_tracks[participant.identity] = []
            
            if track.kind == rtc.TrackKind.KIND_AUDIO:
                self.audio_tracks[participant.identity].append(track)
                logger.info(f"AUDIO TRACK added for {participant.identity}")
                
                if self._is_agent_participant(participant):
                    logger.info("AGENT AUDIO TRACK DETECTED! Starting stream")
                    self._start_agent_audio_stream(participant, track)

    def _handle_participant_joined(self, participant):
        self.participants[participant.identity] = participant
        
        if self._is_agent_participant(participant):
            self.agent_participant = participant
            logger.info(f"AGENT DETECTED: {participant.identity}")
            
            # Cancel timeout task since agent connected
            if self.agent_connection_timeout_task:
                self.agent_connection_timeout_task.cancel()
                logger.info("Agent connection timeout cancelled - agent joined successfully")

    def _is_agent_participant(self, participant):
        identity = participant.identity.lower()
        agent_patterns = [
            "agent-", agent_name.lower(), "ac_", "agent", "assistant", "ai-"
        ]
        return any(pattern in identity for pattern in agent_patterns)

    def _start_agent_audio_stream(self, participant, track):
        if self.audio_stream_task and not self.audio_stream_task.done():
            self.audio_stream_task.cancel()
        
        if not self._is_websocket_open() or not self.session_ready or not self.call_active:
            logger.warning("Cannot start audio stream - connection not ready")
            return
        
        logger.info("Starting agent audio stream")
        self.audio_stream_task = asyncio.create_task(
            self.stream_agent_audio(track, participant.identity)
        )

    async def stream_agent_audio(self, audio_track, participant_identity):
        """Stream agent audio with background mixing"""
        logger.info(f"Starting agent audio stream from {participant_identity}")
        
        frame_count = 0
        
        try:
            audio_stream = rtc.AudioStream(audio_track)
            self.agent_is_speaking = True
            
            async for audio_frame_event in audio_stream:
                if not self._is_websocket_open() or not self.connected_to_livekit or not self.call_active:
                    logger.warning("Connection lost, stopping audio stream")
                    break
                
                frame_count += 1
                
                try:
                    frame = audio_frame_event.frame
                    resampled_frames = self.return_resampler.push(frame)
                    
                    for resampled_frame in resampled_frames:
                        pcm_bytes = bytes(resampled_frame.data[:resampled_frame.samples_per_channel * 2])
                        
                        if ENABLE_AUDIO_OPTIMIZATION:
                            loop = asyncio.get_event_loop()
                            mulaw_bytes = await loop.run_in_executor(
                                audio_processor_pool,
                                process_pcm_to_mulaw,
                                pcm_bytes
                            )
                        else:
                            mulaw_bytes = audioop.lin2ulaw(pcm_bytes, 2)
                        
                        if mulaw_bytes:
                            await self.send_audio_to_maqsam_with_background(mulaw_bytes)
                            self.stats["audio_frames_received_from_agent"] += 1
                            self.stats["bytes_to_maqsam"] += len(mulaw_bytes)
                        
                except Exception as e:
                    logger.error(f"Error processing audio frame {frame_count}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error in agent audio stream: {e}")
        finally:
            self.agent_is_speaking = False
            logger.info(f"Agent audio stream ended. Frames processed: {frame_count}")

    async def send_audio_to_maqsam_with_background(self, agent_audio_data):
        """Send agent audio mixed with background to Maqsam"""
        try:
            if not self._is_websocket_open():
                return False
            
            final_audio = agent_audio_data
            
            # Mix with background audio if available
            if (self.background_audio_manager and 
                self.background_audio_manager.is_running and 
                self.background_audio_manager.background_audio_data):
                
                bg_chunk = self.background_audio_manager.get_audio_chunk(len(agent_audio_data))
                if bg_chunk:
                    final_audio = self._mix_audio_samples(agent_audio_data, bg_chunk)
            
            encoded_audio = base64.b64encode(final_audio).decode('utf-8')
            message = {
                "type": "response.stream",
                "data": {"audio": encoded_audio}
            }
            
            await self.websocket.send(json.dumps(message))
            self.messages_sent += 1
            return True
            
        except websockets.ConnectionClosed:
            logger.warning("WebSocket connection closed during send")
            self.call_active = False
            return False
        except Exception as e:
            logger.error(f"Error sending audio to Maqsam: {e}")
            return False

    def _mix_audio_samples(self, agent_audio, bg_audio):
        """Mix agent audio with background audio"""
        try:
            if not agent_audio or not bg_audio:
                return agent_audio or bg_audio
            
            # Convert both to PCM for mixing
            agent_pcm = audioop.ulaw2lin(agent_audio, 2)
            bg_pcm = audioop.ulaw2lin(bg_audio, 2)
            
            # Ensure same length
            agent_len = len(agent_pcm)
            bg_len = len(bg_pcm)
            
            if bg_len < agent_len:
                repetitions = (agent_len // bg_len) + 1
                bg_pcm = (bg_pcm * repetitions)[:agent_len]
            elif bg_len > agent_len:
                bg_pcm = bg_pcm[:agent_len]
            
            # Convert to sample arrays
            agent_samples = array.array('h')
            bg_samples = array.array('h')
            
            agent_samples.frombytes(agent_pcm)
            bg_samples.frombytes(bg_pcm)
            
            # Mix samples
            mixed_samples = array.array('h')
            for i in range(len(agent_samples)):
                agent_sample = agent_samples[i]
                bg_sample = int(bg_samples[i] * BACKGROUND_VOLUME_RATIO)
                
                # Prevent clipping
                mixed_sample = agent_sample + bg_sample
                mixed_sample = max(-32768, min(32767, mixed_sample))
                mixed_samples.append(mixed_sample)
            
            # Convert back to μ-law
            mixed_pcm = mixed_samples.tobytes()
            return audioop.lin2ulaw(mixed_pcm, 2)
            
        except Exception as e:
            logger.error(f"Error mixing audio: {e}")
            return agent_audio

    async def _stream_background_audio(self):
        """Stream background audio when agent is not speaking"""
        if not self.background_audio_manager or not self.background_audio_manager.background_audio_data:
            logger.warning("No background audio available")
            return
        
        logger.info("Starting background audio stream")
        
        try:
            while self.call_active and self._is_websocket_open():
                if not self.session_ready:
                    await asyncio.sleep(0.02)
                    continue
                
                # Only send background audio when agent is not speaking
                if self.agent_is_speaking:
                    await asyncio.sleep(0.02)
                    continue
                
                # Send background audio chunk
                chunk_size = AUDIO_FRAME_SIZE
                bg_chunk = self.background_audio_manager.get_audio_chunk(chunk_size)
                
                if bg_chunk:
                    encoded_audio = base64.b64encode(bg_chunk).decode('utf-8')
                    message = {
                        "type": "response.stream",
                        "data": {"audio": encoded_audio}
                    }
                    
                    await self.websocket.send(json.dumps(message))
                    self.messages_sent += 1
                
                # 20ms intervals for background audio
                await asyncio.sleep(0.02)
                
        except Exception as e:
            logger.error(f"Error in background audio stream: {e}")

    async def _trigger_agent(self):
        """Launch agent with proper timing and verification"""
        logger.info(f"Triggering agent for room: {self.room_name}")
        
        # Verify we're connected first
        if not self.connected_to_livekit or not self.room:
            logger.error("Cannot trigger agent: Not connected to LiveKit")
            return False
        
        try:
            # Extract calling number from context
            calling_number = "unknown"
            direction = "inbound"
            
            if self.context and isinstance(self.context, dict):
                calling_number = self.context.get('caller_number', 'unknown')
                direction = self.context.get('direction', 'inbound')
            
            logger.info(f"Extracted calling_number: {calling_number}")
            
            # Create metadata with more detail for agent
            metadata = {
                "phone": calling_number,
                "calling_number": calling_number,
                "direction": direction,
                "call_type": "inbound",
                "call_id": self.context.get('id', '') if self.context else '',
                "participant_identity": self.room.local_participant.identity,
                "room_name": self.room_name,
                "context": self.context or {}
            }
            
            logger.info(f"Enhanced metadata: {json.dumps(metadata, indent=2)}")
            
            metadata_json = json.dumps(metadata)
            
            # Use asyncio subprocess for non-blocking execution
            cmd = [
                "lk", "dispatch", "create",
                "--room", self.room_name,
                "--agent-name", agent_name,
                "--metadata", metadata_json
            ]
            
            logger.info(f"Agent dispatch command: {' '.join(cmd[:6])}... [metadata truncated]")
            
            # Run with timeout and capture output
            process = await asyncio.wait_for(
                asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                ),
                timeout=10.0
            )
            
            # Wait for process to complete and get output
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=10.0)
            
            if process.returncode == 0:
                output = stdout.decode().strip()
                logger.info(f"Agent dispatch successful: {output}")
                
                # Extract dispatch ID if available
                if "Dispatch created:" in output:
                    logger.info(f"Agent dispatch details: {output}")
                
                return True
            else:
                error_output = stderr.decode().strip()
                logger.error(f"Agent dispatch failed (code {process.returncode}): {error_output}")
                return False
                
        except asyncio.TimeoutError:
            logger.error(f"Agent dispatch timed out after 10 seconds")
            return False
        except Exception as e:
            logger.error(f"Error launching agent: {e}")
            import traceback
            logger.error(f"Agent dispatch traceback: {traceback.format_exc()}")
            return False

    async def _monitor_agent_with_timeout(self):
        """Monitor for agent connection with timeout"""
        logger.info("Starting agent connection monitoring with timeout")
        
        elapsed = 0
        check_interval = 2
        
        while elapsed < AGENT_CONNECTION_TIMEOUT and self.connected_to_livekit and self.call_active:
            await asyncio.sleep(check_interval)
            elapsed += check_interval
            
            current_participants = len(self.room.remote_participants)
            logger.info(f"Agent monitoring ({elapsed}s): {current_participants} remote participants")
            
            if current_participants > 0:
                for participant in self.room.remote_participants.values():
                    if self._is_agent_participant(participant):
                        logger.info(f"Agent detected after {elapsed}s!")
                        return True
            
            if elapsed % 5 == 0:
                logger.info(f"Still waiting for agent... ({elapsed}s elapsed)")
        
        # Timeout reached - terminate call
        logger.error(f"Agent connection timeout after {AGENT_CONNECTION_TIMEOUT}s - terminating call")
        await self._terminate_call_due_to_timeout()
        return False

    async def _terminate_call_due_to_timeout(self):
        """Terminate call due to agent connection timeout"""
        logger.warning("Terminating call due to agent connection timeout")
        
        try:
            # Send error message to Maqsam if possible
            if self._is_websocket_open():
                error_message = {
                    "type": "session.error",
                    "data": {"error": "Agent connection timeout", "code": "AGENT_TIMEOUT"}
                }
                await self.websocket.send(json.dumps(error_message))
                
            # Mark call as inactive and close connection
            self.call_active = False
            await asyncio.sleep(0.1)  # Brief delay for message to send
            
            if self._is_websocket_open():
                await self.websocket.close(code=1011, reason="Agent timeout")
                
        except Exception as e:
            logger.error(f"Error during timeout termination: {e}")
        
        raise CallTerminationError("Agent connection timeout")

    async def _terminate_call_due_to_agent_disconnect(self):
        """Terminate call due to agent disconnection"""
        logger.warning("Terminating call due to agent disconnection")
        
        try:
            # Send error message to Maqsam if possible
            if self._is_websocket_open():
                error_message = {
                    "type": "session.error", 
                    "data": {"error": "Agent disconnected", "code": "AGENT_DISCONNECTED"}
                }
                await self.websocket.send(json.dumps(error_message))
                
            # Mark call as inactive and close connection
            self.call_active = False
            await asyncio.sleep(0.1)  # Brief delay for message to send
            
            if self._is_websocket_open():
                await self.websocket.close(code=1011, reason="Agent disconnected")
                
        except Exception as e:
            logger.error(f"Error during agent disconnect termination: {e}")

    async def cleanup(self):
        """Clean up resources"""
        logger.info("Starting cleanup...")
        
        self.call_active = False
        
        # Cancel timeout task
        if self.agent_connection_timeout_task and not self.agent_connection_timeout_task.done():
            self.agent_connection_timeout_task.cancel()
        
        # Stop background audio
        if self.background_audio_manager:
            self.background_audio_manager.stop()
        
        # Cancel background streaming task
        if self.background_stream_task and not self.background_stream_task.done():
            self.background_stream_task.cancel()
            try:
                await asyncio.wait_for(self.background_stream_task, timeout=1.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
        
        # Cancel audio streaming task
        if self.audio_stream_task and not self.audio_stream_task.done():
            self.audio_stream_task.cancel()
            try:
                await asyncio.wait_for(self.audio_stream_task, timeout=1.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
        
        # Cleanup audio source
        if self.audio_source:
            try:
                await self.audio_source.cleanup()
            except Exception as e:
                logger.error(f"Error cleaning up audio source: {e}")
        
        # Disconnect from LiveKit
        if self.room and self.connected_to_livekit:
            try:
                self.connected_to_livekit = False
                await asyncio.wait_for(self.room.disconnect(), timeout=2.0)
                logger.info("Disconnected from LiveKit room")
            except Exception as e:
                logger.error(f"Error disconnecting from LiveKit: {e}")
        
        # Log session stats
        elapsed = time.time() - self.connection_start_time
        logger.info(f"Session Summary:")
        logger.info(f"  Duration: {elapsed:.1f}s")
        logger.info(f"  Messages: {self.messages_received} received, {self.messages_sent} sent")
        logger.info(f"  Audio frames: {self.stats['audio_frames_sent_to_livekit']} to LiveKit, {self.stats['audio_frames_received_from_agent']} from Agent")
        
        logger.info("Cleanup complete")

# Connection management functions
async def enforce_connection_limits(websocket):
    """Enforce connection limits"""
    global active_connections
    
    client_ip = websocket.remote_address[0] if websocket.remote_address else "unknown"
    current_time = time.time()
    
    # Rate limiting
    attempts = connection_attempts[client_ip]
    while attempts and attempts[0] < current_time - RATE_LIMIT_WINDOW:
        attempts.popleft()
    
    if len(attempts) >= RATE_LIMIT_PER_IP:
        logger.warning(f"Rate limit exceeded for IP {client_ip}")
        await websocket.close(code=1008, reason="Rate limit exceeded")
        return False
    
    if active_connections >= MAX_CONNECTIONS:
        logger.warning(f"Global connection limit reached: {active_connections}/{MAX_CONNECTIONS}")
        await websocket.close(code=1008, reason="Server at capacity")
        return False
    
    attempts.append(current_time)
    active_connections += 1
    connections_per_ip[client_ip] += 1
    
    logger.info(f"Connection accepted. Total: {active_connections}")
    return True

def cleanup_connection(websocket):
    """Clean up connection tracking"""
    global active_connections
    
    client_ip = websocket.remote_address[0] if websocket.remote_address else "unknown"
    active_connections -= 1
    connections_per_ip[client_ip] -= 1
    
    if connections_per_ip[client_ip] <= 0:
        del connections_per_ip[client_ip]

async def handle_maqsam_websocket(websocket):
    """Main Maqsam WebSocket handler"""
    handler = None
    
    try:
        if not await enforce_connection_limits(websocket):
            return
        
        logger.info(f"NEW WEBSOCKET CONNECTION from {websocket.remote_address}")
        
        # Check for HTTP Auth header
        auth_header = None
        if hasattr(websocket, 'request_headers'):
            headers = dict(websocket.request_headers)
            auth_header = headers.get('auth') or headers.get('Auth') or headers.get('Authorization')
        
        if auth_header:
            if not validate_auth_token(auth_header):
                await websocket.close(code=1008, reason="Invalid auth header")
                return
            logger.info("Authenticated via HTTP Auth header")
        
        handler = OptimizedMaqsamWebSocketHandler(websocket)
        await handler.handle_connection()
        
    except websockets.ConnectionClosed:
        logger.info("Connection closed normally")
    except Exception as e:
        logger.error(f"Error in WebSocket handler: {e}")
    finally:
        cleanup_connection(websocket)
        if handler:
            try:
                await handler.cleanup()
            except Exception as e:
                logger.error(f"Error in handler cleanup: {e}")

async def start_maqsam_websocket_server():
    """Start optimized Maqsam WebSocket server"""
    logger.info("Starting WebSocket server on ws://0.0.0.0:8765")
    
    try:
        async with websockets.serve(
            handle_maqsam_websocket,
            "0.0.0.0", 
            8765,
            max_size=512*1024,
            max_queue=16,
            compression=None,
            ping_interval=60,
            ping_timeout=10,
            close_timeout=5,
        ):
            logger.info("WebSocket server listening on ws://0.0.0.0:8765")
            logger.info(f"Auth token required: {VALID_AUTH_TOKEN}")
            logger.info(f"Max connections: {MAX_CONNECTIONS}")
            logger.info(f"Agent connection timeout: {AGENT_CONNECTION_TIMEOUT}s")
            logger.info(f"Audio frame size: {AUDIO_FRAME_SIZE} samples (20ms)")
            logger.info(f"Background audio: {ENABLE_BACKGROUND_AUDIO}")
            if ENABLE_BACKGROUND_AUDIO and global_background_audio_manager and global_background_audio_manager.background_audio_data:
                logger.info(f"Background file: {BACKGROUND_AUDIO_FILE} (loaded)")
            logger.info("Server ready for connections...")
            
            await asyncio.Future()  # Run forever
            
    except Exception as e:
        logger.error(f"Error starting WebSocket server: {e}")
        raise

async def start_http_server():
    """Start HTTP server for health checks"""
    
    async def handle_health(request):
        return web.json_response({
            "status": "healthy",
            "timestamp": time.time(),
            "service": "optimized-maqsam-livekit-bridge",
            "version": "3.0-robust-error-handling",
            "connections": {
                "active": active_connections,
                "max": MAX_CONNECTIONS
            },
            "config": {
                "agent_timeout": AGENT_CONNECTION_TIMEOUT,
                "audio_frame_size": AUDIO_FRAME_SIZE,
                "background_audio": ENABLE_BACKGROUND_AUDIO,
                "telephony_sample_rate": TELEPHONY_SAMPLE_RATE,
                "livekit_sample_rate": LIVEKIT_SAMPLE_RATE
            },
            "error_handling": {
                "agent_timeout_enabled": True,
                "agent_disconnect_termination": True,
                "graceful_cleanup": True
            }
        })

    async def handle_stats(request):
        uptime = time.time() - server_start_time
        
        return web.json_response({
            "uptime_seconds": uptime,
            "connections": {
                "current": active_connections,
                "total_handled": total_connections_handled
            },
            "performance": {
                "thread_pool_size": PROCESS_POOL_SIZE,
                "audio_optimizations": ENABLE_AUDIO_OPTIMIZATION
            }
        })

    app = web.Application()
    app.router.add_get("/health", handle_health)
    app.router.add_get("/stats", handle_stats)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8080)
    await site.start()
    
    logger.info("HTTP server listening on http://0.0.0.0:8080")

async def monitor_connections():
    """Monitor connections periodically"""
    while True:
        await asyncio.sleep(300)  # Every 5 minutes
        
        # Cleanup old connection attempts
        current_time = time.time()
        for ip in list(connection_attempts.keys()):
            attempts = connection_attempts[ip]
            while attempts and attempts[0] < current_time - RATE_LIMIT_WINDOW:
                attempts.popleft()
            if not attempts:
                del connection_attempts[ip]

# Global statistics
server_start_time = time.time()
total_connections_handled = 0

async def main():
    """Main function to run optimized Maqsam integration"""
    global total_connections_handled, global_background_audio_manager
    
    logger.info("Starting Optimized Maqsam-LiveKit Bridge v3.0...")
    logger.info("=" * 80)
    
    # Initialize global background audio manager
    if ENABLE_BACKGROUND_AUDIO:
        logger.info("Pre-loading background audio...")
        global_background_audio_manager = BackgroundAudioManager(BACKGROUND_AUDIO_FILE)
        if global_background_audio_manager.background_audio_data:
            logger.info("Background audio loaded successfully")
        else:
            logger.warning("Background audio failed to load")
            global_background_audio_manager = None
    
    # Log configuration
    logger.info("Configuration:")
    logger.info(f"  LiveKit URL: {LIVEKIT_URL}")
    logger.info(f"  Auth Token: {VALID_AUTH_TOKEN}")
    logger.info(f"  Agent: {agent_name}")
    logger.info(f"  Agent Timeout: {AGENT_CONNECTION_TIMEOUT}s")
    logger.info(f"  Audio: {TELEPHONY_SAMPLE_RATE}Hz <-> {LIVEKIT_SAMPLE_RATE}Hz")
    logger.info(f"  Frame Size: {AUDIO_FRAME_SIZE} samples")
    logger.info(f"  Background Audio: {ENABLE_BACKGROUND_AUDIO}")
    logger.info(f"  Max Connections: {MAX_CONNECTIONS}")
    logger.info("Error Handling: Agent timeout + disconnect termination enabled")
    logger.info("=" * 80)
    
    try:
        monitor_task = asyncio.create_task(monitor_connections())
        
        # Track connections
        original_enforce = enforce_connection_limits
        async def tracked_enforce_limits(websocket):
            global total_connections_handled
            result = await original_enforce(websocket)
            if result:
                total_connections_handled += 1
            return result
        
        globals()['enforce_connection_limits'] = tracked_enforce_limits
        
        await asyncio.gather(
            start_maqsam_websocket_server(),
            start_http_server(),
            return_exceptions=True
        )
        
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
        monitor_task.cancel()
        audio_processor_pool.shutdown(wait=True)
        
    except Exception as e:
        logger.error(f"Server error: {e}")
        if 'monitor_task' in locals():
            monitor_task.cancel()
        audio_processor_pool.shutdown(wait=True)
        raise

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down service...")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        if 'audio_processor_pool' in globals():
            audio_processor_pool.shutdown(wait=True)
        logger.info("Optimized Maqsam-LiveKit Bridge stopped")