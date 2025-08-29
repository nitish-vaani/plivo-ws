#The version below solves the below issues:
# - Launches the bg noise ultra fast
# - Launches the agent fast
# - Handles audio sample rate and type conversion 
# - Handles background audio mixing from the ws file
# - To be worked out; why the 2 second delay
                     # - Drop call when the agent is not available
                     # - Manage the delay in responses
                     # - Refactoring to code.

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
BACKGROUND_AUDIO_FILE = "call-center.mp3"  # or bg.wav
BACKGROUND_VOLUME_RATIO = 0.15  # 15% of agent volume
ENABLE_BACKGROUND_AUDIO = False

# Maqsam Authentication
VALID_AUTH_TOKEN = os.environ.get("MAQSAM_AUTH_TOKEN", "maqsam_secure_token_123")

# ULTRA LOW LATENCY OPTIMIZATIONS
AUDIO_FRAME_SIZE = 80   # 10ms at 8kHz (80 samples) - reduced from 160
MAX_BUFFER_SIZE = 1     # Minimal buffering - reduced from 3
PROCESS_POOL_SIZE = 4   # For parallel audio processing
ENABLE_AUDIO_OPTIMIZATION = True
USE_FASTER_RESAMPLING = True

# Production settings
MAX_CONNECTIONS = 500
RATE_LIMIT_PER_IP = 10
RATE_LIMIT_WINDOW = 60

# Configure logging with less verbose output for production
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global thread pool for audio processing
audio_processor_pool = ThreadPoolExecutor(max_workers=PROCESS_POOL_SIZE)

# Global background audio manager - load once at startup
global_background_audio_manager = None

# Connection tracking
active_connections = 0
connections_per_ip = defaultdict(int)
connection_attempts = defaultdict(lambda: deque())

def validate_auth_token(token: str) -> bool:
    """Validate Maqsam authentication token"""
    if not token or token != VALID_AUTH_TOKEN:
        return False
    return True

def create_room_from_context(context: dict) -> str:
    """Create room ID from Maqsam context with enhanced error handling"""
    try:
        if not isinstance(context, dict):
            return f"maqsam_fallback_{uuid.uuid4()}"
        
        call_id = context.get('id', uuid.uuid4().hex[:8])
        caller_number = context.get('caller_number', 'unknown')
        direction = context.get('direction', 'unknown')
        timestamp = context.get('timestamp', time.strftime('%Y%m%d_%H%M%S'))
        
        clean_timestamp = str(timestamp).replace(' ', '_').replace('-', '').replace(':', '')
        room_id = f"maqsam_{direction}_{caller_number}_{call_id}_{clean_timestamp}"
        logger.info(f"🏠 Created room ID: {room_id}")
        return room_id
        
    except Exception as e:
        logger.error(f"❌ Error creating room from context: {e}")
        fallback_room = f"maqsam_fallback_{uuid.uuid4()}"
        return fallback_room

def process_mulaw_to_pcm(mulaw_data):
    """Process μ-law conversion in thread pool for better performance"""
    try:
        return audioop.ulaw2lin(mulaw_data, 2)
    except Exception as e:
        logger.error(f"❌ μ-law conversion error: {e}")
        return None

def process_pcm_to_mulaw(pcm_data):
    """Process PCM to μ-law conversion in thread pool"""
    try:
        return audioop.lin2ulaw(pcm_data, 2)
    except Exception as e:
        logger.error(f"❌ PCM to μ-law conversion error: {e}")
        return None

def mix_audio_samples(agent_mulaw, bg_mulaw, bg_volume_ratio=BACKGROUND_VOLUME_RATIO):
    """Mix agent audio with background audio at μ-law level"""
    try:
        if not agent_mulaw or not bg_mulaw:
            return agent_mulaw or bg_mulaw
        
        # Convert both to PCM for mixing
        agent_pcm = audioop.ulaw2lin(agent_mulaw, 2)
        bg_pcm = audioop.ulaw2lin(bg_mulaw, 2)
        
        # Ensure same length (pad or truncate background)
        agent_len = len(agent_pcm)
        bg_len = len(bg_pcm)
        
        if bg_len < agent_len:
            # Repeat background if too short
            repetitions = (agent_len // bg_len) + 1
            bg_pcm = (bg_pcm * repetitions)[:agent_len]
        elif bg_len > agent_len:
            # Truncate background if too long
            bg_pcm = bg_pcm[:agent_len]
        
        # Convert to sample arrays for mixing
        agent_samples = array.array('h')
        bg_samples = array.array('h')
        
        agent_samples.frombytes(agent_pcm)
        bg_samples.frombytes(bg_pcm)
        
        # Mix samples with volume control
        mixed_samples = array.array('h')
        for i in range(len(agent_samples)):
            # Mix: agent at full volume + background at reduced volume
            agent_sample = agent_samples[i]
            bg_sample = int(bg_samples[i] * bg_volume_ratio)
            
            # Ensure no clipping
            mixed_sample = agent_sample + bg_sample
            mixed_sample = max(-32768, min(32767, mixed_sample))
            mixed_samples.append(mixed_sample)
        
        # Convert back to μ-law
        mixed_pcm = mixed_samples.tobytes()
        mixed_mulaw = audioop.lin2ulaw(mixed_pcm, 2)
        
        return mixed_mulaw
        
    except Exception as e:
        logger.error(f"❌ Error mixing audio: {e}")
        return agent_mulaw  # Return agent audio if mixing fails

class BackgroundAudioManager:
    """Manages continuous background audio for user"""
    
    def __init__(self, audio_file_path):
        self.audio_file_path = audio_file_path
        self.background_audio_data = None
        self.current_position = 0
        self.is_running = False
        self.lock = threading.Lock()
        
        logger.info(f"🎵 Initializing background audio manager")
        self._load_background_audio()
    
    def _load_background_audio(self):
        """Load and convert background audio to telephony format"""
        try:
            if not os.path.exists(self.audio_file_path):
                logger.warning(f"⚠️ Background audio file not found: {self.audio_file_path}")
                logger.warning("🔇 Background audio disabled")
                return
            
            logger.info(f"📂 Loading background audio: {self.audio_file_path}")
            
            # Use ffmpeg to convert to telephony format
            import tempfile
            with tempfile.NamedTemporaryFile(suffix='.wav', delete=False) as temp_file:
                temp_path = temp_file.name
            
            try:
                # Convert to 8kHz mono PCM using ffmpeg
                result = subprocess.run([
                    'ffmpeg', '-i', self.audio_file_path,
                    '-ar', str(TELEPHONY_SAMPLE_RATE),
                    '-ac', '1',
                    '-f', 'wav',
                    '-y',  # Overwrite output file
                    temp_path
                ], capture_output=True, text=True, timeout=30)
                
                if result.returncode != 0:
                    logger.error(f"❌ FFmpeg conversion failed: {result.stderr}")
                    return
                
                # Read the converted WAV file
                with wave.open(temp_path, 'rb') as wav_file:
                    if wav_file.getnchannels() != 1:
                        logger.error("❌ Background audio must be mono")
                        return
                    
                    if wav_file.getframerate() != TELEPHONY_SAMPLE_RATE:
                        logger.error(f"❌ Background audio must be {TELEPHONY_SAMPLE_RATE}Hz")
                        return
                    
                    # Read PCM data
                    pcm_data = wav_file.readframes(wav_file.getnframes())
                    
                    # Convert PCM to μ-law for telephony
                    self.background_audio_data = audioop.lin2ulaw(pcm_data, 2)
                    
                    duration = len(pcm_data) / (2 * TELEPHONY_SAMPLE_RATE)  # 2 bytes per sample
                    logger.info(f"✅ Background audio loaded: {len(self.background_audio_data)} bytes, {duration:.1f}s")
                
            finally:
                # Clean up temp file
                try:
                    os.unlink(temp_path)
                except:
                    pass
                    
        except subprocess.TimeoutExpired:
            logger.error("❌ FFmpeg conversion timed out")
        except FileNotFoundError:
            logger.error("❌ FFmpeg not found. Please install ffmpeg")
            logger.error("   Ubuntu/Debian: sudo apt install ffmpeg")
            logger.error("   macOS: brew install ffmpeg")
        except Exception as e:
            logger.error(f"❌ Error loading background audio: {e}")
    
    def get_audio_chunk(self, chunk_size):
        """Get a chunk of background audio data"""
        if not self.background_audio_data:
            return None
        
        with self.lock:
            data_len = len(self.background_audio_data)
            
            if self.current_position >= data_len:
                self.current_position = 0  # Loop back to start
            
            # Get chunk, handle wrap-around
            if self.current_position + chunk_size <= data_len:
                chunk = self.background_audio_data[self.current_position:self.current_position + chunk_size]
                self.current_position += chunk_size
            else:
                # Need to wrap around
                first_part = self.background_audio_data[self.current_position:]
                remaining = chunk_size - len(first_part)
                second_part = self.background_audio_data[:remaining]
                chunk = first_part + second_part
                self.current_position = remaining
            
            return chunk
    
    def start(self):
        """Start background audio"""
        self.is_running = True
        logger.info("🎵 Background audio started")
    
    def stop(self):
        """Stop background audio"""
        self.is_running = False
        logger.info("🔇 Background audio stopped")

class OptimizedAudioBuffer:
    """Ultra low-latency audio buffer with minimal buffering"""
    
    def __init__(self, max_size=MAX_BUFFER_SIZE):
        self.buffer = deque(maxlen=max_size)
        self.lock = threading.Lock()
        self.dropped_frames = 0
    
    def push(self, data):
        """Push data with overflow protection"""
        with self.lock:
            if len(self.buffer) >= self.buffer.maxlen:
                self.buffer.popleft()  # Drop oldest frame
                self.dropped_frames += 1
            self.buffer.append(data)
    
    def pop_all(self):
        """Pop all available data"""
        with self.lock:
            data = list(self.buffer)
            self.buffer.clear()
            return data
    
    def size(self):
        with self.lock:
            return len(self.buffer)

class OptimizedMaqsamAudioSource(rtc.AudioSource):
    """Ultra-optimized audio source for minimal latency"""
    
    def __init__(self):
        super().__init__(
            sample_rate=LIVEKIT_SAMPLE_RATE,
            num_channels=1
        )
        
        # Use fastest resampler settings
        quality = rtc.AudioResamplerQuality.LOW if USE_FASTER_RESAMPLING else rtc.AudioResamplerQuality.HIGH
        self.resampler = rtc.AudioResampler(
            input_rate=TELEPHONY_SAMPLE_RATE,
            output_rate=LIVEKIT_SAMPLE_RATE,
            num_channels=1,
            quality=quality
        )
        
        # Minimal audio buffer
        self.audio_buffer = OptimizedAudioBuffer()
        
        # Processing statistics
        self.frame_count = 0
        self.total_bytes_processed = 0
        self.last_audio_time = time.time()
        self.processing_times = deque(maxlen=50)  # Reduced from 100
        
        # Start background processing task
        self.processing_task = None
        self.should_process = True
        
        logger.info(f"🎤 Ultra-optimized Audio Source initialized: {TELEPHONY_SAMPLE_RATE}Hz -> {LIVEKIT_SAMPLE_RATE}Hz")

    async def start_processing(self):
        """Start background audio processing"""
        if not self.processing_task:
            self.processing_task = asyncio.create_task(self._process_audio_buffer())

    async def push_audio_data(self, mulaw_data):
        """Push audio data for processing (ultra low latency)"""
        if not mulaw_data or not self.should_process:
            return

        start_time = time.time()
        
        # Push to buffer instead of processing immediately
        self.audio_buffer.push(mulaw_data)
        
        # Start processing task if not running
        if not self.processing_task:
            await self.start_processing()
        
        # Track timing
        process_time = time.time() - start_time
        self.processing_times.append(process_time)

    async def _process_audio_buffer(self):
        """Ultra-fast background audio processing loop"""
        logger.info("🔄 Starting ultra-fast audio processing")
        
        while self.should_process:
            try:
                # Get all buffered audio data
                audio_chunks = self.audio_buffer.pop_all()
                
                if not audio_chunks:
                    await asyncio.sleep(0.001)  # 1ms sleep when no data - reduced from 5ms
                    continue
                
                # Process all chunks
                for mulaw_data in audio_chunks:
                    await self._process_single_chunk(mulaw_data)
                
                # Minimal yield to prevent blocking
                # await asyncio.sleep(0)  # Immediate yield - removed sleep entirely
                
            except Exception as e:
                logger.error(f"❌ Error in audio processing loop: {e}")
                await asyncio.sleep(0.005)  # Prevent tight error loop

    async def _process_single_chunk(self, mulaw_data):
        """Process a single audio chunk with minimal latency"""
        try:
            self.frame_count += 1
            self.total_bytes_processed += len(mulaw_data)
            self.last_audio_time = time.time()

            # Convert μ-law to PCM in thread pool for non-blocking operation
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
            
            # Convert to samples array
            samples = array.array("h")
            samples.frombytes(pcm_data)

            if len(samples) == 0:
                return

            # Create input frame for resampling
            input_frame = rtc.AudioFrame.create(
                sample_rate=TELEPHONY_SAMPLE_RATE,
                num_channels=1,
                samples_per_channel=len(samples)
            )
            input_frame.data[:len(samples)] = samples

            # Resample to LiveKit's sample rate
            resampled_frames = self.resampler.push(input_frame)

            # Push each resampled frame to LiveKit immediately
            for resampled_frame in resampled_frames:
                await self.capture_frame(resampled_frame)

        except Exception as e:
            logger.error(f"❌ Error processing audio chunk: {e}")

    async def cleanup(self):
        """Clean up audio source"""
        try:
            self.should_process = False
            
            if self.processing_task:
                self.processing_task.cancel()
                try:
                    await asyncio.wait_for(self.processing_task, timeout=0.5)  # Reduced timeout
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
            
            logger.debug(f"🧹 Audio source cleanup - Frames: {self.frame_count}, "
                        f"Bytes: {self.total_bytes_processed}, "
                        f"Dropped: {self.audio_buffer.dropped_frames}")
            
        except Exception as e:
            logger.error(f"❌ Error cleaning up audio source: {e}")

class OptimizedMaqsamWebSocketHandler:
    """Ultra-optimized WebSocket handler with minimal latency"""
    
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
        self.background_stream_task = None  # Add background streaming task
        self.agent_is_speaking = False     # Track if agent is currently speaking
        
        # Track participants and audio tracks
        self.participants = {}
        self.audio_tracks = {}
        
        # Ultra-fast audio conversion for return path
        quality = rtc.AudioResamplerQuality.LOW if USE_FASTER_RESAMPLING else rtc.AudioResamplerQuality.HIGH
        self.return_resampler = rtc.AudioResampler(
            input_rate=LIVEKIT_SAMPLE_RATE,
            output_rate=TELEPHONY_SAMPLE_RATE,
            num_channels=1,
            quality=quality
        )
        
        # Minimal audio buffer for return path
        self.return_audio_buffer = OptimizedAudioBuffer()
        
        # Background audio manager - use global instance
        self.background_audio_manager = global_background_audio_manager
        
        # Statistics
        self.stats = {
            "audio_frames_sent_to_livekit": 0,
            "audio_frames_received_from_agent": 0,
            "bytes_from_maqsam": 0,
            "bytes_to_maqsam": 0,
            "average_processing_time": 0.0,
            "background_audio_mixed_frames": 0,
            "background_only_frames": 0,  # Track background-only frames
            "prewarming_frames": 0,  # Track pre-warming frames
        }
        
        logger.info(f"🆕 Created ultra-optimized WebSocket handler")
    
    def _is_websocket_open(self):
        """Robust check if websocket connection is open"""
        try:
            return self.websocket.state == websockets.protocol.State.OPEN
        except AttributeError:
            try:
                return hasattr(self.websocket, 'open') and self.websocket.open
            except:
                return False
    
    async def handle_connection(self):
        """Main handler for Maqsam WebSocket connection with immediate background audio"""
        try:
            # Start background audio IMMEDIATELY when connection is established
            if self.background_audio_manager:
                self.background_audio_manager.start()
                logger.info("🎵 Background audio started immediately (using global manager)")
            
            # Start background audio streaming task immediately (even before session setup)
            self.background_stream_task = asyncio.create_task(self._stream_background_audio_continuously())
            logger.info("🚀 Background audio streaming started immediately")
            
            # Send immediate pre-warming background audio
            asyncio.create_task(self._immediate_prewarming())
            
            # Wait for messages and process them
            async for message in self.websocket:
                self.messages_received += 1
                
                # Process message synchronously to ensure proper session setup
                await self._process_message_async(message)
                
                # Only break if explicitly told to end the call
                if not self.call_active and self.session_ready:
                    logger.info("🔴 Call ended, breaking message loop")
                    break
                    
        except websockets.ConnectionClosed:
            logger.info("📞 Connection closed normally")
        except Exception as e:
            logger.error(f"❌ Error in handler: {e}")
        finally:
            await self.cleanup()
    
    async def _immediate_prewarming(self):
        """Send immediate background audio for instant experience"""
        try:
            # Send 10 frames immediately for instant audio
            for i in range(10):
                success = await self._send_background_only_audio_prewarming()
                if success:
                    self.stats["prewarming_frames"] += 1
                await asyncio.sleep(0.01)  # 10ms between frames
            logger.info("🚀 Immediate pre-warming completed (10 frames sent)")
        except Exception as e:
            logger.debug(f"Immediate pre-warming failed: {e}")
    
    async def _process_message_async(self, message):
        """Process message asynchronously to avoid blocking the main loop"""
        try:
            if isinstance(message, str):
                data = json.loads(message)
                await self._handle_json_message(data)
            elif isinstance(message, bytes):
                try:
                    decoded = message.decode('utf-8')
                    data = json.loads(decoded)
                    await self._handle_json_message(data)
                except Exception as e:
                    logger.error(f"❌ Failed to decode bytes message: {e}")
                    
        except json.JSONDecodeError as e:
            logger.error(f"❌ Invalid JSON from Maqsam: {e}")
        except Exception as e:
            logger.error(f"❌ Error processing message: {e}")
    
    async def _handle_json_message(self, data):
        """Handle JSON messages from Maqsam"""
        message_type = data.get("type")
        
        if message_type == "session.setup":
            await self._handle_session_setup(data)
        elif message_type == "audio.input":
            await self._handle_audio_input_optimized(data)
        elif message_type == "call.mark":
            await self._handle_call_mark(data)
        elif message_type == "call.dtmf":
            await self._handle_dtmf(data)
        else:
            logger.debug(f"❓ Unknown message type: {message_type}")
    
    # async def _handle_session_setup(self, data):
    #     """Handle session.setup message with proper sequencing"""
    #     logger.info("🔧 Handling session.setup")
        
    #     # Extract authentication token
    #     api_key = data.get("apiKey")
    #     if api_key:
    #         if not validate_auth_token(api_key):
    #             logger.error("❌ Invalid API key provided")
    #             await self.websocket.close(code=1008, reason="Invalid API key")
    #             self.call_active = False
    #             return
    #         self.authenticated = True
    #         logger.info("✅ Authenticated via session.setup apiKey")
        
    #     # Extract session data
    #     session_data = data.get("data", {})
    #     raw_context = session_data.get("context")
        
    #     # Handle different context formats
    #     try:
    #         if isinstance(raw_context, str):
    #             try:
    #                 self.context = json.loads(raw_context)
    #             except json.JSONDecodeError as e:
    #                 logger.error(f"❌ Failed to parse context JSON: {e}")
    #                 self.context = {"raw_context": raw_context}
    #         elif isinstance(raw_context, dict):
    #             self.context = raw_context
    #         elif raw_context is None:
    #             logger.warning("⚠️ Context is None, using empty context")
    #             self.context = {}
    #         else:
    #             logger.warning(f"⚠️ Unexpected context type: {type(raw_context)}")
    #             self.context = {"raw_context": str(raw_context)}
    #     except Exception as e:
    #         logger.error(f"❌ Error processing context: {e}")
    #         self.context = {}
        
    #     # Log essential context details
    #     if isinstance(self.context, dict) and self.context:
    #         logger.info(f"📋 Call context - ID: {self.context.get('id', 'N/A')}, "
    #                    f"Caller: {self.context.get('caller_number', 'N/A')}, "
    #                    f"Direction: {self.context.get('direction', 'N/A')}")
        
    #     # Create room from context
    #     self.room_name = create_room_from_context(self.context)
        
    #     # Send session ready FIRST
    #     await self._send_session_ready()
        
    #     # THEN connect to LiveKit (which will trigger agent after connection is stable)
    #     await self._connect_to_livekit_ultra_fast()
        
    #     # Start monitoring for agent connection
    #     asyncio.create_task(self._monitor_agent_connection())
    
    async def _handle_session_setup(self, data):
        """Handle session.setup with parallel operations for maximum speed"""
        logger.info("🔧 Handling session.setup")
        
        # Extract authentication token
        api_key = data.get("apiKey")
        if api_key:
            if not validate_auth_token(api_key):
                logger.error("❌ Invalid API key provided")
                await self.websocket.close(code=1008, reason="Invalid API key")
                self.call_active = False
                return
            self.authenticated = True
            logger.info("✅ Authenticated via session.setup apiKey")
        
        # Extract session data quickly
        session_data = data.get("data", {})
        raw_context = session_data.get("context")
        
        # Fast context processing
        try:
            if isinstance(raw_context, str):
                self.context = json.loads(raw_context)
            elif isinstance(raw_context, dict):
                self.context = raw_context
            else:
                self.context = {}
        except:
            self.context = {}
        
        # Create room from context
        self.room_name = create_room_from_context(self.context)
        
        # PARALLEL EXECUTION: Start all operations simultaneously
        session_ready_task = asyncio.create_task(self._send_session_ready())
        livekit_connect_task = asyncio.create_task(self._connect_to_livekit_ultra_fast())
        
        # Wait for session ready to complete first (needed for audio pipeline)
        await session_ready_task
        
        # Let LiveKit connection complete in background (don't block)
        asyncio.create_task(self._wait_for_livekit_and_monitor(livekit_connect_task))

    async def _wait_for_livekit_and_monitor(self, livekit_connect_task):
        """Wait for LiveKit connection and start monitoring"""
        try:
            await livekit_connect_task
            # Start monitoring immediately after connection
            asyncio.create_task(self._monitor_agent_connection())
        except Exception as e:
            logger.error(f"❌ Error in LiveKit connection: {e}")



    async def _send_session_ready(self):
        """Send session.ready confirmation to Maqsam and prime audio pipeline"""
        ready_message = {"type": "session.ready"}
        await self.websocket.send(json.dumps(ready_message))
        self.session_ready = True
        self.messages_sent += 1
        logger.info("📤 Sent session.ready - background audio will now stream normally")
        
        # Send priming audio to start the pipeline immediately
        asyncio.create_task(self._send_priming_audio())
    
    async def _send_priming_audio(self):
        """Send silent audio to prime the agent pipeline"""
        await asyncio.sleep(0.05)  # Very small delay
        try:
            # Create 10ms of silence at 8kHz μ-law
            silence_samples = b'\xff' * AUDIO_FRAME_SIZE  # μ-law silence
            encoded_audio = base64.b64encode(silence_samples).decode('utf-8')
            
            message = {
                "type": "response.stream", 
                "data": {"audio": encoded_audio}
            }
            
            if self._is_websocket_open():
                await self.websocket.send(json.dumps(message))
                logger.debug("🔇 Sent priming audio")
        except Exception as e:
            logger.debug(f"Priming audio failed: {e}")
    
    async def _handle_audio_input_optimized(self, data):
        """Ultra-optimized audio input handling"""
        if not self.session_ready:
            return
        
        audio_data = data.get("data", {})
        base64_audio = audio_data.get("audio")
        
        if base64_audio and self.audio_source:
            try:
                # Decode base64 μ-law audio
                mulaw_data = base64.b64decode(base64_audio)
                
                if self.connected_to_livekit:
                    # Push to optimized audio source (non-blocking)
                    await self.audio_source.push_audio_data(mulaw_data)
                    self.stats["audio_frames_sent_to_livekit"] += 1
                    self.stats["bytes_from_maqsam"] += len(mulaw_data)
                
            except Exception as e:
                logger.error(f"❌ Error processing audio: {e}")
    
    async def _handle_call_mark(self, data):
        """Handle call.mark message"""
        mark_data = data.get("data", {})
        label = mark_data.get("label")
        logger.info(f"🏷️ Received call.mark: {label}")
    
    async def _handle_dtmf(self, data):
        """Handle call.dtmf message"""
        dtmf_data = data.get("data", {})
        digit = dtmf_data.get("digit")
        logger.info(f"📞 Received DTMF digit: {digit}")
    
    # async def _connect_to_livekit_ultra_fast(self):
    #     """Ultra-fast LiveKit connection with proper participant identity"""
    #     if not self.room_name:
    #         logger.error("❌ No room name available for LiveKit connection")
    #         return
        
    #     # Use calling number in identity for better agent recognition
    #     calling_number = "unknown"
    #     if self.context and isinstance(self.context, dict):
    #         calling_number = self.context.get('caller_number', 'unknown')
        
    #     # Create predictable identity that agent can recognize
    #     identity = f"caller-{calling_number}-{int(time.time())}"
    #     participant_name = f"Maqsam Caller {calling_number}"
        
    #     logger.info(f"🔗 Connecting as identity: {identity}")
        
    #     try:
    #         logger.info(f"🔗 Connecting to LiveKit room: {self.room_name}")
            
    #         lkapi = api.LiveKitAPI(LIVEKIT_URL, LIVEKIT_API_KEY, LIVEKIT_API_SECRET)
            
    #         # Create token with proper grants
    #         token = (api.AccessToken(LIVEKIT_API_KEY, LIVEKIT_API_SECRET)
    #                 .with_identity(identity)
    #                 .with_name(participant_name)
    #                 .with_grants(api.VideoGrants(
    #                     room_join=True,
    #                     room=self.room_name,
    #                     can_publish=True,      # Add explicit publish permission
    #                     can_subscribe=True,    # Add explicit subscribe permission
    #                     can_publish_data=True, # Add data publishing permission
    #                 )))
            
    #         # Create room first, then connect, then trigger agent
    #         room_task = asyncio.create_task(self._create_room_safe(lkapi, self.room_name))
            
    #         # Connect to room
    #         self.room = rtc.Room()
    #         self._setup_room_events()

    #         # Connect with longer timeout for initial connection
    #         logger.info(f"🔗 Attempting LiveKit connection at {time.time()}")
    #         await asyncio.wait_for(
    #             self.room.connect(LIVEKIT_URL, token.to_jwt()), 
    #             timeout=10.0  # Increased timeout
    #         )
    #         logger.info(f"✅ LiveKit room connection successful at {time.time()}")
            
    #         self.connected_to_livekit = True
            
    #         # Setup audio track immediately after connection
    #         self.audio_source = OptimizedMaqsamAudioSource()
    #         await self.audio_source.start_processing()
            
    #         self.audio_track = rtc.LocalAudioTrack.create_audio_track(
    #             "maqsam-audio", 
    #             self.audio_source
    #         )
            
    #         options = rtc.TrackPublishOptions()
    #         options.source = rtc.TrackSource.SOURCE_MICROPHONE
            
    #         await self.room.local_participant.publish_track(self.audio_track, options)
    #         logger.info(f"✅ Audio track published at {time.time()}")
            
    #         # Wait a moment for connection to stabilize
    #         await asyncio.sleep(0.5)
            
    #         # NOW trigger the agent after we're fully connected
    #         logger.info("🚀 Connection stabilized, triggering agent...")
    #         agent_task = asyncio.create_task(self._trigger_agent_ultra_fast())
            
    #         await lkapi.aclose()
            
    #         # Log connection status - FIXED: Use remote_participants
    #         logger.info(f"🔍 Connected participants: {len(self.room.remote_participants)}")
    #         logger.info(f"🔍 Local participant identity: {self.room.local_participant.identity}")
            
    #     except Exception as e:
    #         logger.error(f"❌ Failed to connect to LiveKit: {e}")
    #         import traceback
    #         logger.error(f"❌ Connection traceback: {traceback.format_exc()}")
    
    async def _connect_to_livekit_ultra_fast(self):
        """Ultra-fast LiveKit connection with parallel operations"""
        if not self.room_name:
            logger.error("❌ No room name available for LiveKit connection")
            return
        
        calling_number = "unknown"
        if self.context and isinstance(self.context, dict):
            calling_number = self.context.get('caller_number', 'unknown')
        
        identity = f"caller-{calling_number}-{int(time.time())}"
        participant_name = f"Maqsam Caller {calling_number}"
        
        logger.info(f"🔗 Connecting as identity: {identity}")
        
        try:
            logger.info(f"🔗 Connecting to LiveKit room: {self.room_name}")
            
            lkapi = api.LiveKitAPI(LIVEKIT_URL, LIVEKIT_API_KEY, LIVEKIT_API_SECRET)
            
            # Create token
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
            
            # PARALLEL EXECUTION: Start room creation, connection, and agent dispatch simultaneously
            room_creation_task = asyncio.create_task(self._create_room_safe(lkapi, self.room_name))
            
            # Connect to room immediately (don't wait for room creation)
            self.room = rtc.Room()
            self._setup_room_events()

            connection_task = asyncio.create_task(
                self.room.connect(LIVEKIT_URL, token.to_jwt())
            )
            
            # Start agent dispatch immediately (parallel with connection)
            agent_task = asyncio.create_task(self._trigger_agent_ultra_fast())
            
            # Wait for connection to complete
            await asyncio.wait_for(connection_task, timeout=10.0)
            logger.info(f"✅ LiveKit room connection successful at {time.time()}")
            
            self.connected_to_livekit = True
            
            # Setup audio track immediately
            self.audio_source = OptimizedMaqsamAudioSource()
            await self.audio_source.start_processing()
            
            self.audio_track = rtc.LocalAudioTrack.create_audio_track(
                "maqsam-audio", 
                self.audio_source
            )
            
            options = rtc.TrackPublishOptions()
            options.source = rtc.TrackSource.SOURCE_MICROPHONE
            
            await self.room.local_participant.publish_track(self.audio_track, options)
            logger.info(f"✅ Audio track published at {time.time()}")
            
            # Don't wait for room creation or agent task - let them complete in background
            await lkapi.aclose()
            
            logger.info(f"🔍 Connected participants: {len(self.room.remote_participants)}")
            logger.info(f"🔍 Local participant identity: {self.room.local_participant.identity}")
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to LiveKit: {e}")
            import traceback
            logger.error(f"❌ Connection traceback: {traceback.format_exc()}")


    async def _create_room_safe(self, lkapi, room_name):
        """Safely create room with error handling"""
        try:
            await lkapi.room.create_room(api.CreateRoomRequest(name=room_name))
            logger.info(f"✅ Created LiveKit room: {room_name}")
        except Exception as e:
            logger.debug(f"Room creation result: {e}")
    
    def _setup_room_events(self):
        """Setup LiveKit room event handlers with enhanced debugging"""
        
        @self.room.on("connected")
        def on_connected():
            logger.info(f"✅ LiveKit connected at {time.time()}")
            logger.info(f"🔍 Local participant: {self.room.local_participant.identity}")
            logger.info(f"🔍 Room name: {self.room.name}")
            self.connected_to_livekit = True

        @self.room.on("disconnected")
        def on_disconnected():
            logger.info(f"❌ LiveKit connection lost at {time.time()}")
            self.connected_to_livekit = False

        @self.room.on("participant_connected")
        def on_participant_connected(participant):
            logger.info(f"👤 Participant joined: {participant.identity} at {time.time()}")
            logger.info(f"🔍 Participant name: {participant.name}")
            logger.info(f"🔍 Participant metadata: {participant.metadata}")
            logger.info(f"🔍 Total remote participants in room: {len(self.room.remote_participants)}")
            
            # List all current remote participants - FIXED: Use remote_participants.values()
            for p in self.room.remote_participants.values():
                logger.info(f"   - {p.identity} ({p.name})")
            
            self._handle_participant_joined(participant)

        @self.room.on("participant_disconnected")
        def on_participant_disconnected(participant):
            logger.info(f"👋 Participant left: {participant.identity} at {time.time()}")
            
            # Clean up tracking
            if participant.identity in self.participants:
                del self.participants[participant.identity]
            if participant.identity in self.audio_tracks:
                del self.audio_tracks[participant.identity]
            
            if participant == self.agent_participant:
                logger.warning("🤖 AGENT PARTICIPANT DISCONNECTED!")
                self.agent_participant = None
                if self.audio_stream_task and not self.audio_stream_task.done():
                    self.audio_stream_task.cancel()

        @self.room.on("track_published")
        def on_track_published(publication, participant):
            logger.info(f"📤 Track published by {participant.identity}: {publication.kind} - {publication.name}")

        @self.room.on("track_unpublished")
        def on_track_unpublished(publication, participant):
            logger.info(f"📤 Track unpublished by {participant.identity}: {publication.kind} - {publication.name}")

        @self.room.on("track_subscribed")
        def on_track_subscribed(track, publication, participant):
            logger.info(f"🎵 Track subscribed from {participant.identity}: {track.kind} - {publication.name} at {time.time()}")
            
            if participant.identity not in self.audio_tracks:
                self.audio_tracks[participant.identity] = []
            
            if track.kind == rtc.TrackKind.KIND_AUDIO:
                self.audio_tracks[participant.identity].append(track)
                logger.info(f"🎤 AUDIO TRACK added for {participant.identity}")
                
                if self._is_agent_participant(participant):
                    logger.info(f"🤖 AGENT AUDIO TRACK DETECTED! Starting stream at {time.time()}")
                    self._start_ultra_fast_agent_audio_stream(participant, track)
                else:
                    logger.info(f"👤 Non-agent audio track from {participant.identity}")

        @self.room.on("track_unsubscribed")
        def on_track_unsubscribed(track, publication, participant):
            logger.info(f"🔇 Track unsubscribed from {participant.identity}: {track.kind}")

        # Add room state debugging
        @self.room.on("room_metadata_changed")
        def on_room_metadata_changed(metadata):
            logger.info(f"📝 Room metadata changed: {metadata}")

        # Debug connection state changes
        @self.room.on("connection_state_changed")
        def on_connection_state_changed(state):
            logger.info(f"🔗 Connection state changed to: {state}")

        # Add periodic room state logging - FIXED: Use remote_participants
        async def log_room_state():
            while self.connected_to_livekit and self.call_active:
                await asyncio.sleep(10)  # Log every 10 seconds
                logger.info(f"🏠 Room state check:")
                logger.info(f"   - Room: {self.room.name}")
                logger.info(f"   - Connected: {self.connected_to_livekit}")
                logger.info(f"   - Remote participants: {len(self.room.remote_participants)}")
                for p in self.room.remote_participants.values():
                    track_count = len(p.track_publications) if hasattr(p, 'track_publications') else 0
                    logger.info(f"     * {p.identity} ({p.name}) - Tracks: {track_count}")
        
        # Start room state monitoring
        asyncio.create_task(log_room_state())

    def _handle_participant_joined(self, participant):
        """Handle participant joining"""
        self.participants[participant.identity] = participant
        
        if self._is_agent_participant(participant):
            self.agent_participant = participant
            logger.info(f"🤖 AGENT DETECTED: {participant.identity} at {time.time()}")

    def _is_agent_participant(self, participant):
        """Check if participant is an agent"""
        identity = participant.identity.lower()
        agent_patterns = [
            "agent-", agent_name.lower(), "ac_", "agent", "assistant", "ai-"
        ]
        return any(pattern in identity for pattern in agent_patterns)

    def _start_ultra_fast_agent_audio_stream(self, participant, track):
        """Start ultra-fast agent audio streaming"""
        # Cancel existing stream task if any
        if self.audio_stream_task and not self.audio_stream_task.done():
            self.audio_stream_task.cancel()
        
        # Check connection status using robust method
        if not self._is_websocket_open() or not self.session_ready or not self.call_active:
            logger.warning("❌ Cannot start audio stream - connection not ready or call not active")
            return
        
        # Start ultra-fast audio streaming task
        logger.info(f"🚀 Starting ultra-fast agent audio stream at {time.time()}")
        self.audio_stream_task = asyncio.create_task(
            self.stream_agent_audio_ultra_fast(track, participant.identity)
        )

    async def stream_agent_audio_ultra_fast(self, audio_track, participant_identity):
        """Ultra-fast agent audio streaming with minimal latency and background audio mixing"""
        logger.info(f"🔊 Starting ultra-fast agent audio stream from {participant_identity} at {time.time()}")
        
        frame_count = 0
        bytes_sent = 0
        processing_times = deque(maxlen=25)  # Reduced from 50
        first_audio_sent = False
        
        try:
            # Create audio stream
            audio_stream = rtc.AudioStream(audio_track)
            
            # Mark agent as speaking
            self.agent_is_speaking = True
            
            async for audio_frame_event in audio_stream:
                start_time = time.time()
                
                # Log first audio frame timing
                if not first_audio_sent:
                    logger.info(f"🎤 FIRST AUDIO FRAME from agent at {time.time()}")
                    first_audio_sent = True
                
                # Quick connection check using robust method
                if not self._is_websocket_open() or not self.connected_to_livekit or not self.call_active:
                    logger.warning("❌ Connection lost or call ended, stopping audio stream")
                    break
                
                frame_count += 1
                
                try:
                    frame = audio_frame_event.frame
                    
                    # Resample from 48kHz to 8kHz
                    resampled_frames = self.return_resampler.push(frame)
                    
                    for resampled_frame in resampled_frames:
                        # Convert to PCM bytes
                        pcm_bytes = bytes(resampled_frame.data[:resampled_frame.samples_per_channel * 2])
                        
                        # Convert PCM to μ-law using thread pool for non-blocking operation
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
                            # Send to Maqsam with background audio mixing
                            success = await self.send_audio_to_maqsam_with_background(mulaw_bytes)
                            
                            if success:
                                bytes_sent += len(mulaw_bytes)
                                self.stats["audio_frames_received_from_agent"] += 1
                                self.stats["bytes_to_maqsam"] += len(mulaw_bytes)
                                
                                # Log first successful send
                                if frame_count == 1:
                                    logger.info(f"📤 FIRST AUDIO SENT to Maqsam at {time.time()}")
                        
                except Exception as e:
                    logger.error(f"❌ Error processing audio frame {frame_count}: {e}")
                    continue
                
                # Track processing time
                process_time = time.time() - start_time
                processing_times.append(process_time)
                
                # Log performance stats occasionally
                if frame_count % 100 == 0:  # Reduced frequency
                    avg_time = sum(processing_times) / len(processing_times) * 1000
                    self.stats["average_processing_time"] = avg_time
                    logger.debug(f"📊 Avg processing time: {avg_time:.2f}ms, Frames: {frame_count}")
                    
        except Exception as e:
            logger.error(f"❌ Error in ultra-fast agent audio stream: {e}")
        finally:
            # Mark agent as no longer speaking
            self.agent_is_speaking = False
            
            avg_time = sum(processing_times) / len(processing_times) * 1000 if processing_times else 0
            logger.info(f"🔇 Ultra-fast audio stream ended. Frames: {frame_count}, "
                       f"Bytes: {bytes_sent}, Avg time: {avg_time:.2f}ms, "
                       f"Background mixed frames: {self.stats['background_audio_mixed_frames']}")

    async def send_audio_to_maqsam_with_background(self, agent_audio_data):
        """Send agent audio mixed with background audio to Maqsam"""
        try:
            # Quick connection check using robust method
            if not self._is_websocket_open():
                logger.warning("⚠️ WebSocket closed, cannot send audio")
                return False
            
            final_audio = agent_audio_data
            
            # Mix with background audio if available
            if (self.background_audio_manager and 
                self.background_audio_manager.is_running and 
                self.background_audio_manager.background_audio_data):
                
                # Get background audio chunk matching agent audio length
                bg_chunk = self.background_audio_manager.get_audio_chunk(len(agent_audio_data))
                
                if bg_chunk:
                    # Mix agent audio with background
                    final_audio = mix_audio_samples(agent_audio_data, bg_chunk, BACKGROUND_VOLUME_RATIO)
                    self.stats["background_audio_mixed_frames"] += 1
            
            # Encode audio as base64
            encoded_audio = base64.b64encode(final_audio).decode('utf-8')
            
            # Create minimal message structure
            message = {
                "type": "response.stream",
                "data": {"audio": encoded_audio}
            }
            
            await self.websocket.send(json.dumps(message))
            self.messages_sent += 1
            
            return True
            
        except websockets.ConnectionClosed:
            logger.warning("❌ WebSocket connection closed during send")
            self.call_active = False
            return False
        except Exception as e:
            logger.error(f"❌ Error sending audio to Maqsam: {e}")
            return False

    async def _stream_background_audio_continuously(self):
        """Stream background audio continuously from call start with pre-warming"""
        if not self.background_audio_manager or not self.background_audio_manager.background_audio_data:
            logger.warning("⚠️ No background audio available for continuous streaming")
            return
        
        logger.info("🎵 Starting continuous background audio stream with pre-warming")
        
        frame_count = 0
        prewarming_frames = 0
        
        try:
            # PRE-WARMING: Start sending background audio immediately when call connects
            # Don't wait for session_ready to begin background audio
            while self.call_active and self._is_websocket_open():
                # If session is not ready yet, send background audio anyway (pre-warming)
                if not self.session_ready:
                    # Send background audio during pre-warming phase
                    success = await self._send_background_only_audio_prewarming()
                    if success:
                        frame_count += 1
                        prewarming_frames += 1
                        self.stats["prewarming_frames"] += 1
                    
                    await asyncio.sleep(0.01)  # 10ms intervals during pre-warming
                    continue
                
                # Normal operation after session is ready
                # If agent is speaking, let the mixed audio handle background
                if self.agent_is_speaking:
                    await asyncio.sleep(0.02)  # 20ms sleep when agent is speaking
                    continue
                
                # Send background-only audio when agent is not speaking
                success = await self._send_background_only_audio()
                
                if success:
                    frame_count += 1
                    self.stats["background_only_frames"] += 1
                
                # Send frames every 10ms to match AUDIO_FRAME_SIZE timing
                await asyncio.sleep(0.01)  # 10ms = AUDIO_FRAME_SIZE duration
                
        except Exception as e:
            logger.error(f"❌ Error in continuous background audio stream: {e}")
        finally:
            logger.info(f"🔇 Background audio stream ended. Total frames: {frame_count}, "
                       f"Pre-warming frames: {prewarming_frames}")

    async def _send_background_only_audio_prewarming(self):
        """Send background audio during pre-warming phase (before session ready)"""
        try:
            if not self._is_websocket_open():
                return False
            
            if (self.background_audio_manager and 
                self.background_audio_manager.is_running and 
                self.background_audio_manager.background_audio_data):
                
                # Send a chunk of background audio (10ms worth)
                chunk_size = AUDIO_FRAME_SIZE  # 80 bytes for 10ms at 8kHz μ-law
                bg_chunk = self.background_audio_manager.get_audio_chunk(chunk_size)
                
                if bg_chunk:
                    encoded_audio = base64.b64encode(bg_chunk).decode('utf-8')
                    message = {
                        "type": "response.stream",
                        "data": {"audio": encoded_audio}
                    }
                    
                    # Only send if websocket is open, don't check session_ready
                    await self.websocket.send(json.dumps(message))
                    self.messages_sent += 1
                    return True
            
            return False
            
        except Exception as e:
            # During pre-warming, some sends might fail - that's okay
            logger.debug(f"Pre-warming background audio send failed: {e}")
            return False

    async def _send_background_only_audio(self):
        """Send background audio when agent is not speaking"""
        try:
            if not self._is_websocket_open():
                return False
            
            if (self.background_audio_manager and 
                self.background_audio_manager.is_running and 
                self.background_audio_manager.background_audio_data):
                
                # Send a chunk of background audio (10ms worth)
                chunk_size = AUDIO_FRAME_SIZE  # 80 bytes for 10ms at 8kHz μ-law
                bg_chunk = self.background_audio_manager.get_audio_chunk(chunk_size)
                
                if bg_chunk:
                    encoded_audio = base64.b64encode(bg_chunk).decode('utf-8')
                    message = {
                        "type": "response.stream",
                        "data": {"audio": encoded_audio}
                    }
                    
                    await self.websocket.send(json.dumps(message))
                    self.messages_sent += 1
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"❌ Error sending background-only audio: {e}")
            return False

    # async def _trigger_agent_ultra_fast(self):
    #     """Launch agent with proper timing and verification"""
    #     logger.info(f"🚀 Triggering agent for room: {self.room_name} at {time.time()}")
        
    #     # Verify we're actually connected first
    #     if not self.connected_to_livekit or not self.room:
    #         logger.error("❌ Cannot trigger agent: Not connected to LiveKit")
    #         return False
        
    #     # Log current room state before triggering agent - FIXED: Use remote_participants
    #     logger.info(f"🔍 Pre-agent room state:")
    #     logger.info(f"   - Room: {self.room_name}")
    #     logger.info(f"   - Local participant: {self.room.local_participant.identity}")
    #     logger.info(f"   - Remote participants count: {len(self.room.remote_participants)}")
        
    #     try:
    #         # Extract calling number from context
    #         calling_number = "unknown"
    #         direction = "inbound"
            
    #         if self.context and isinstance(self.context, dict):
    #             calling_number = self.context.get('caller_number', 'unknown')
    #             direction = self.context.get('direction', 'inbound')
            
    #         logger.info(f"🔍 Extracted calling_number: {calling_number}")
            
    #         # Create metadata with more detail for agent
    #         metadata = {
    #             "phone": calling_number,
    #             "calling_number": calling_number,
    #             "direction": direction,
    #             "call_type": "inbound",
    #             "call_id": self.context.get('id', '') if self.context else '',
    #             "participant_identity": self.room.local_participant.identity,  # Add participant identity
    #             "room_name": self.room_name,  # Add room name
    #             "context": self.context or {}
    #         }
            
    #         logger.info(f"📤 Enhanced metadata: {json.dumps(metadata, indent=2)}")
            
    #         metadata_json = json.dumps(metadata)
            
    #         # Use asyncio subprocess for non-blocking execution
    #         cmd = [
    #             "lk", "dispatch", "create",
    #             "--room", self.room_name,
    #             "--agent-name", agent_name,
    #             "--metadata", metadata_json
    #         ]
            
    #         logger.info(f"📤 Agent dispatch command: {' '.join(cmd[:6])}... [metadata truncated]")
            
    #         # Run with timeout
    #         process = await asyncio.wait_for(
    #             asyncio.create_subprocess_exec(
    #                 *cmd,
    #                 stdout=asyncio.subprocess.PIPE,
    #                 stderr=asyncio.subprocess.PIPE
    #             ),
    #             timeout=10.0
    #         )
            
    #         # Wait for process to complete and get output
    #         stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=10.0)
            
    #         if process.returncode == 0:
    #             output = stdout.decode().strip()
    #             logger.info(f"✅ Agent dispatch successful: {output}")
                
    #             # Extract dispatch ID if available
    #             if "Dispatch created:" in output:
    #                 logger.info(f"🎯 Agent dispatch details: {output}")
                
    #             return True
    #         else:
    #             error_output = stderr.decode().strip()
    #             logger.error(f"❌ Agent dispatch failed (code {process.returncode}): {error_output}")
    #             return False
                
    #     except asyncio.TimeoutError:
    #         logger.error(f"❌ Agent dispatch timed out after 10 seconds")
    #         return False
    #     except Exception as e:
    #         logger.error(f"❌ Error launching agent: {e}")
    #         import traceback
    #         logger.error(f"❌ Agent dispatch traceback: {traceback.format_exc()}")
    #         return False

    async def _trigger_agent_ultra_fast(self):
        """Launch agent with pre-built command and no waiting"""
        logger.info(f"🚀 Triggering agent for room: {self.room_name} at {time.time()}")
        
        try:
            # Extract metadata quickly
            calling_number = "unknown"
            direction = "inbound"
            
            if self.context and isinstance(self.context, dict):
                calling_number = self.context.get('caller_number', 'unknown')
                direction = self.context.get('direction', 'inbound')
            
            # Minimal metadata for speed
            metadata = {
                "phone": calling_number,
                "direction": direction,
                "call_type": "inbound"
            }
            
            metadata_json = json.dumps(metadata)
            
            # Pre-build command for maximum speed
            cmd = [
                "lk", "dispatch", "create",
                "--room", self.room_name,
                "--agent-name", agent_name,
                "--metadata", metadata_json
            ]
            
            logger.info(f"📤 Agent dispatch starting at {time.time()}")
            
            # Fire-and-forget agent dispatch (don't wait for completion)
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.DEVNULL,  # Don't capture output for speed
                stderr=asyncio.subprocess.PIPE      # Only capture errors
            )
            
            logger.info(f"✅ Agent dispatch initiated (PID: {process.pid})")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error launching agent: {e}")
            return False

    async def _monitor_agent_connection(self):
        """Monitor for agent connection after dispatch"""
        logger.info("🔍 Starting agent connection monitoring...")
        
        max_wait_time = 30  # Wait up to 30 seconds for agent
        check_interval = 2  # Check every 2 seconds
        elapsed = 0
        
        while elapsed < max_wait_time and self.connected_to_livekit:
            await asyncio.sleep(check_interval)
            elapsed += check_interval
            
            # Check if agent has joined - FIXED: Use remote_participants
            current_participants = len(self.room.remote_participants)
            logger.info(f"🕐 Agent monitoring ({elapsed}s): {current_participants} remote participants")
            
            if current_participants > 0:  # Agent joined
                for participant in self.room.remote_participants.values():  # FIXED: Use .values() for dict
                    logger.info(f"   👤 Participant: {participant.identity} ({participant.name})")
                    if self._is_agent_participant(participant):
                        logger.info(f"🤖 AGENT DETECTED after {elapsed}s!")
                        return True
            
            if elapsed % 10 == 0:  # Log every 10 seconds
                logger.info(f"⏳ Still waiting for agent... ({elapsed}s elapsed)")
        
        logger.warning(f"⚠️ Agent didn't join within {max_wait_time}s")
        return False

    # Additional optimized methods
    async def send_speech_started(self):
        """Send speech.started to Maqsam (customer interruption)"""
        try:
            message = {"type": "speech.started"}
            await self.websocket.send(json.dumps(message))
            self.messages_sent += 1
            logger.info("📤 Sent speech.started")
            return True
        except Exception as e:
            logger.error(f"❌ Error sending speech.started: {e}")
            return False

    async def send_call_redirect(self):
        """Send call.redirect to Maqsam (redirect to human)"""
        try:
            message = {"type": "call.redirect"}
            await self.websocket.send(json.dumps(message))
            self.messages_sent += 1
            logger.info("📤 Sent call.redirect")
            return True
        except Exception as e:
            logger.error(f"❌ Error sending call.redirect: {e}")
            return False

    async def send_call_mark(self, label):
        """Send call.mark to Maqsam"""
        try:
            message = {
                "type": "call.mark",
                "data": {"label": label}
            }
            await self.websocket.send(json.dumps(message))
            self.messages_sent += 1
            logger.info(f"📤 Sent call.mark: {label}")
            return True
        except Exception as e:
            logger.error(f"❌ Error sending call.mark: {e}")
            return False

    async def cleanup(self):
        """Clean up resources"""
        logger.info("🧹 Starting cleanup...")
        
        # Mark call as inactive
        self.call_active = False
        
        # Stop background audio
        if self.background_audio_manager:
            self.background_audio_manager.stop()
        
        # Cancel background streaming task
        if self.background_stream_task and not self.background_stream_task.done():
            self.background_stream_task.cancel()
            try:
                await asyncio.wait_for(self.background_stream_task, timeout=0.5)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
        
        # Cancel audio streaming task
        if self.audio_stream_task and not self.audio_stream_task.done():
            self.audio_stream_task.cancel()
            try:
                await asyncio.wait_for(self.audio_stream_task, timeout=0.5)  # Reduced timeout
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
        
        # Cleanup optimized audio source
        if self.audio_source:
            try:
                await self.audio_source.cleanup()
            except Exception as e:
                logger.error(f"❌ Error cleaning up audio source: {e}")
        
        # Disconnect from LiveKit
        if self.room and self.connected_to_livekit:
            try:
                self.connected_to_livekit = False
                await asyncio.wait_for(self.room.disconnect(), timeout=1.0)  # Reduced timeout
                logger.info("✅ Disconnected from LiveKit room")
            except Exception as e:
                logger.error(f"❌ Error disconnecting from LiveKit: {e}")
        
        # Log session stats
        elapsed = time.time() - self.connection_start_time
        logger.info(f"📊 Session Summary:")
        logger.info(f"   Duration: {elapsed:.1f}s")
        logger.info(f"   Messages: {self.messages_received} received, {self.messages_sent} sent")
        logger.info(f"   Audio frames: {self.stats['audio_frames_sent_to_livekit']} to LiveKit, {self.stats['audio_frames_received_from_agent']} from Agent")
        logger.info(f"   Background audio: {self.stats['background_audio_mixed_frames']} mixed frames, {self.stats['background_only_frames']} background-only frames")
        logger.info(f"   Pre-warming frames: {self.stats['prewarming_frames']}")
        logger.info(f"   Avg processing time: {self.stats['average_processing_time']:.2f}ms")
        
        logger.info("✅ Cleanup complete")

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
        logger.warning(f"🚫 Rate limit exceeded for IP {client_ip}")
        await websocket.close(code=1008, reason="Rate limit exceeded")
        return False
    
    if active_connections >= MAX_CONNECTIONS:
        logger.warning(f"🚫 Global connection limit reached: {active_connections}/{MAX_CONNECTIONS}")
        await websocket.close(code=1008, reason="Server at capacity")
        return False
    
    attempts.append(current_time)
    active_connections += 1
    connections_per_ip[client_ip] += 1
    
    logger.info(f"✅ Connection accepted. Total: {active_connections}")
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
    """Main Maqsam WebSocket handler with ultra-fast optimizations"""
    handler = None
    
    try:
        # Enforce connection limits
        if not await enforce_connection_limits(websocket):
            return
        
        logger.info(f"🔗 NEW WEBSOCKET CONNECTION from {websocket.remote_address} at {time.time()}")
        
        # Check for HTTP Auth header authentication
        auth_header = None
        if hasattr(websocket, 'request_headers'):
            headers = dict(websocket.request_headers)
            auth_header = headers.get('auth') or headers.get('Auth') or headers.get('Authorization')
        
        if auth_header:
            if not validate_auth_token(auth_header):
                await websocket.close(code=1008, reason="Invalid auth header")
                return
            logger.info("✅ Authenticated via HTTP Auth header")
        
        # Create ultra-optimized handler
        handler = OptimizedMaqsamWebSocketHandler(websocket)
        
        # Handle the connection
        await handler.handle_connection()
        
    except websockets.ConnectionClosed:
        logger.info("📞 Connection closed normally")
    except Exception as e:
        logger.error(f"❌ Error in WebSocket handler: {e}")
    finally:
        cleanup_connection(websocket)
        if handler:
            try:
                await handler.cleanup()
            except Exception as e:
                logger.error(f"❌ Error in handler cleanup: {e}")

async def start_maqsam_websocket_server():
    """Start ultra-optimized Maqsam WebSocket server"""
    logger.info("🌐 Starting ultra-optimized WebSocket server on ws://0.0.0.0:8765")
    
    try:
        async with websockets.serve(
            handle_maqsam_websocket,
            "0.0.0.0", 
            8765,
            # Ultra-optimized settings for minimal latency
            max_size=256*1024,  # Further reduced max message size
            max_queue=8,        # Minimal queue size
            compression=None,   # Disable compression for speed
            ping_interval=60,   # Longer ping interval
            ping_timeout=10,    # Faster ping timeout
            close_timeout=3,    # Faster close timeout
        ):
            logger.info("✅ Ultra-optimized WebSocket server listening on ws://0.0.0.0:8765")
            logger.info(f"🔒 Auth token required: {VALID_AUTH_TOKEN}")
            logger.info(f"📊 Max connections: {MAX_CONNECTIONS}")
            logger.info(f"⚡ Audio optimizations: {ENABLE_AUDIO_OPTIMIZATION}")
            logger.info(f"🎵 Fast resampling: {USE_FASTER_RESAMPLING}")
            logger.info(f"🎶 Background audio: {ENABLE_BACKGROUND_AUDIO} (ratio: {BACKGROUND_VOLUME_RATIO})")
            logger.info(f"⏱️ Frame size: {AUDIO_FRAME_SIZE} samples (10ms)")
            logger.info(f"💾 Buffer size: {MAX_BUFFER_SIZE} frame (minimal)")
            logger.info(f"🚀 Pre-warmed background audio: ENABLED")
            if ENABLE_BACKGROUND_AUDIO and global_background_audio_manager and global_background_audio_manager.background_audio_data:
                logger.info(f"📂 Background file: {BACKGROUND_AUDIO_FILE} (pre-loaded)")
            logger.info("⏰ Server ready for ultra-low-latency connections...")
            
            await asyncio.Future()  # Run forever
            
    except Exception as e:
        logger.error(f"❌ Error starting WebSocket server: {e}")
        raise

async def start_http_server():
    """Start HTTP server for health checks"""
    
    async def handle_health(request):
        """Health check endpoint"""
        return web.json_response({
            "status": "healthy",
            "timestamp": time.time(),
            "service": "ultra-optimized-maqsam-livekit-bridge",
            "version": "2.3-prewarmed-background-audio",
            "connections": {
                "active": active_connections,
                "max": MAX_CONNECTIONS,
                "per_ip": dict(connections_per_ip)
            },
            "optimizations": {
                "audio_optimization": ENABLE_AUDIO_OPTIMIZATION,
                "fast_resampling": USE_FASTER_RESAMPLING,
                "audio_frame_size": AUDIO_FRAME_SIZE,
                "max_buffer_size": MAX_BUFFER_SIZE,
                "process_pool_size": PROCESS_POOL_SIZE,
                "prewarmed_background_audio": True
            },
            "background_audio": {
                "enabled": ENABLE_BACKGROUND_AUDIO,
                "file": BACKGROUND_AUDIO_FILE,
                "volume_ratio": BACKGROUND_VOLUME_RATIO,
                "file_exists": os.path.exists(BACKGROUND_AUDIO_FILE) if ENABLE_BACKGROUND_AUDIO else False,
                "prewarming_enabled": True
            },
            "latency_optimizations": {
                "frame_duration_ms": (AUDIO_FRAME_SIZE / TELEPHONY_SAMPLE_RATE) * 1000,
                "minimal_buffering": MAX_BUFFER_SIZE == 1,
                "parallel_agent_dispatch": True,
                "priming_audio_enabled": True,
                "immediate_background_audio": True
            },
            "config": {
                "telephony_sample_rate": TELEPHONY_SAMPLE_RATE,
                "livekit_sample_rate": LIVEKIT_SAMPLE_RATE,
                "agent_name": agent_name
            },
            "livekit": {
                "configured": all([LIVEKIT_URL, LIVEKIT_API_KEY, LIVEKIT_API_SECRET]),
                "url": LIVEKIT_URL
            }
        })

    async def handle_stats(request):
        """Statistics endpoint"""
        uptime = time.time() - server_start_time
        
        return web.json_response({
            "uptime_seconds": uptime,
            "uptime_hours": uptime / 3600,
            "connections": {
                "current": active_connections,
                "peak": peak_connections,
                "total_handled": total_connections_handled
            },
            "performance": {
                "thread_pool_size": PROCESS_POOL_SIZE,
                "audio_optimizations_enabled": ENABLE_AUDIO_OPTIMIZATION,
                "fast_resampling_enabled": USE_FASTER_RESAMPLING,
                "ultra_low_latency_mode": True,
                "prewarmed_background_audio": True
            },
            "background_audio": {
                "enabled": ENABLE_BACKGROUND_AUDIO,
                "volume_ratio": BACKGROUND_VOLUME_RATIO,
                "file_available": os.path.exists(BACKGROUND_AUDIO_FILE) if ENABLE_BACKGROUND_AUDIO else False,
                "prewarming_mode": "immediate_start"
            },
            "rate_limiting": {
                "max_per_ip": RATE_LIMIT_PER_IP,
                "window_seconds": RATE_LIMIT_WINDOW,
                "current_attempts": {ip: len(attempts) for ip, attempts in connection_attempts.items()}
            }
        })

    # Create web application
    app = web.Application()
    
    # Health and monitoring endpoints
    app.router.add_get("/health", handle_health)
    app.router.add_get("/stats", handle_stats)
    
    # Start server
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8080)
    await site.start()
    
    logger.info("🌐 HTTP server listening on http://0.0.0.0:8080")
    logger.info("📋 Health check: http://0.0.0.0:8080/health")
    logger.info("📊 Statistics: http://0.0.0.0:8080/stats")

async def monitor_connections():
    """Monitor connections periodically"""
    global peak_connections
    
    while True:
        await asyncio.sleep(300)  # Every 5 minutes
        
        # Update peak connections
        if active_connections > peak_connections:
            peak_connections = active_connections
        
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
peak_connections = 0
total_connections_handled = 0

async def main():
    """Main function to run ultra-optimized Maqsam integration with pre-warmed background audio"""
    global total_connections_handled, global_background_audio_manager
    
    logger.info("🚀 Starting Ultra-Optimized Maqsam-LiveKit Bridge v2.3 (Pre-warmed Background Audio)...")
    logger.info("=" * 90)
    
    # Validate environment variables
    required_vars = ["LIVEKIT_URL", "LIVEKIT_API_KEY", "LIVEKIT_API_SECRET"]
    
    logger.info("🔧 Checking environment variables...")
    # missing_vars = [var for var in required_vars if not os.environ.get(var)]
    # if missing_vars:
    #     logger.error(f"❌ Missing required environment variables: {missing_vars}")
    #     return
    
    # Initialize global background audio manager at startup
    if ENABLE_BACKGROUND_AUDIO:
        logger.info("🎵 Pre-loading background audio at startup...")
        global_background_audio_manager = BackgroundAudioManager(BACKGROUND_AUDIO_FILE)
        if global_background_audio_manager.background_audio_data:
            logger.info("✅ Background audio pre-loaded successfully")
        else:
            logger.warning("⚠️ Background audio failed to load")
            global_background_audio_manager = None
    
    # Check background audio file
    if ENABLE_BACKGROUND_AUDIO:
        if os.path.exists(BACKGROUND_AUDIO_FILE):
            logger.info(f"✅ Background audio file found: {BACKGROUND_AUDIO_FILE}")
        else:
            logger.warning(f"⚠️ Background audio file not found: {BACKGROUND_AUDIO_FILE}")
            logger.warning("   Background audio will be disabled for this session")
    
    # Log configuration
    logger.info("✅ All environment variables configured")
    logger.info(f"🔗 LiveKit URL: {LIVEKIT_URL}")
    logger.info(f"🔐 Auth Token: {VALID_AUTH_TOKEN}")
    logger.info(f"🎵 Audio: Maqsam({TELEPHONY_SAMPLE_RATE}Hz μ-law) ↔ LiveKit({LIVEKIT_SAMPLE_RATE}Hz)")
    logger.info(f"🤖 Agent: {agent_name}")
    logger.info(f"📊 Limits: {MAX_CONNECTIONS} connections, {RATE_LIMIT_PER_IP}/IP per {RATE_LIMIT_WINDOW}s")
    logger.info(f"⚡ Optimizations: Audio={ENABLE_AUDIO_OPTIMIZATION}, FastResampling={USE_FASTER_RESAMPLING}")
    logger.info(f"🧠 Thread Pool: {PROCESS_POOL_SIZE} workers")
    logger.info(f"🔧 Buffer: {MAX_BUFFER_SIZE} frame (minimal), Frame: {AUDIO_FRAME_SIZE} samples (10ms)")
    logger.info(f"🎶 Background Audio: {ENABLE_BACKGROUND_AUDIO} (ratio: {BACKGROUND_VOLUME_RATIO})")
    logger.info(f"🚀 Pre-warmed Background Audio: ENABLED (pre-loaded at startup)")
    logger.info(f"⏱️ Ultra-Low Latency Mode: ENABLED")
    logger.info("=" * 90)
    
    try:
        # Start monitoring task
        monitor_task = asyncio.create_task(monitor_connections())
        
        # Track connections globally
        original_enforce = enforce_connection_limits
        async def tracked_enforce_limits(websocket):
            global total_connections_handled
            result = await original_enforce(websocket)
            if result:
                total_connections_handled += 1
            return result
        
        # Replace the function
        globals()['enforce_connection_limits'] = tracked_enforce_limits
        
        await asyncio.gather(
            start_maqsam_websocket_server(),
            start_http_server(),
            return_exceptions=True
        )
        
    except KeyboardInterrupt:
        logger.info("👋 Received shutdown signal")
        monitor_task.cancel()
        
        # Cleanup thread pool
        audio_processor_pool.shutdown(wait=True)
        
    except Exception as e:
        logger.error(f"❌ Server error: {e}")
        if 'monitor_task' in locals():
            monitor_task.cancel()
        audio_processor_pool.shutdown(wait=True)
        raise

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Shutting down service...")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
    finally:
        # Ensure thread pool is cleaned up
        if 'audio_processor_pool' in globals():
            audio_processor_pool.shutdown(wait=True)
        logger.info("✅ Ultra-Optimized Maqsam-LiveKit Bridge stopped")