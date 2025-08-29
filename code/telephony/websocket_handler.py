"""
Enhanced WebSocket handler with agent timeout and graceful shutdown
"""
import asyncio
import time
import logging
import websockets
from livekit import rtc

from audio.telephony_audio_source import TelephonyAudioSource
from audio.audio_processor import AudioProcessor
from lk_utils.livekit_manager import LiveKitManager
from agents.agent_manager import AgentManager
from telephony.plivo_handler import PlivoMessageHandler
from telephony.agent_monitor import AgentConnectionMonitor

logger = logging.getLogger(__name__)


class TelephonyWebSocketHandler:
    """Enhanced WebSocket handler with agent timeout and graceful shutdown"""
    
    def __init__(self, room_name, websocket, agent_name=None, noise_settings=None):
        self.room_name = room_name
        self.websocket = websocket
        self.agent_name = agent_name  # Dynamic agent name from URL
        self.connection_start_time = time.time()
        
        # Component managers
        self.livekit_manager = LiveKitManager(room_name)
        self.agent_manager = AgentManager()
        self.plivo_handler = PlivoMessageHandler()
        self.audio_processor = AudioProcessor()
        
        # Agent monitoring
        self.agent_monitor = None  # Will be created in initialize()
        
        # Apply noise settings if provided
        if noise_settings:
            self.audio_processor.update_noise_settings(**noise_settings)
        
        # Start background audio immediately
        if self.audio_processor.get_noise_status()["enabled"]:
            self.audio_processor.start_background_audio()
            # Start continuous background streaming
            self.background_stream_task = asyncio.create_task(self._stream_background_audio_continuously())
        
        # Audio components
        self.audio_source = None
        self.audio_track = None
        self.audio_stream_task = None
        self.background_stream_task = None  # For continuous background audio
        self.agent_is_speaking = False      # Track agent speaking state
        
        # CLEANUP STATE - Critical for proper shutdown
        self.cleanup_started = False
        self.force_stop = False
        
        # Participant tracking
        self.agent_participant = None
        self.participants = {}
        self.audio_tracks = {}
        
        # Statistics
        self.stats = {
            "audio_frames_sent_to_livekit": 0,
            "audio_frames_received_from_agent": 0,
            "bytes_from_telephony": 0,
            "bytes_to_telephony": 0,
        }
        
        logger.info(f"🆕 Created telephony WebSocket handler for room: {room_name}")
        if agent_name:
            logger.info(f"🤖 Using custom agent: {agent_name}")
        
        # Log noise status
        noise_status = self.audio_processor.get_noise_status()
        if noise_status["enabled"]:
            logger.info(f"🔊 Background noise enabled: {noise_status['noise_type']} at volume {noise_status['volume']}")
        else:
            logger.info("🔇 Background noise disabled")
        
    async def initialize(self):
        """Initialize with dynamic agent timeout"""
        logger.info(f"🚀 Starting concurrent setup with 5-second agent timeout...")
        
        # Create agent connection monitor
        self.agent_monitor = AgentConnectionMonitor(self, timeout_seconds=5)
        
        # Start all tasks concurrently
        livekit_task = asyncio.create_task(self._setup_livekit())
        agent_task = asyncio.create_task(self.agent_manager.trigger_agent(self.room_name, self.agent_name))
        message_task = asyncio.create_task(self._handle_messages())
        
        # Start agent connection monitoring
        monitor_task = asyncio.create_task(self.agent_monitor.start_monitoring())
        
        # Wait for LiveKit connection
        try:
            success = await asyncio.wait_for(livekit_task, timeout=8.0)
            if success:
                logger.info(f"✅ LiveKit connected for room: {self.room_name}")
            else:
                logger.error("❌ LiveKit connection failed")
        except asyncio.TimeoutError:
            logger.error("❌ LiveKit connection timeout (8s)")
        
        # Wait for agent dispatch to complete
        try:
            await asyncio.wait_for(agent_task, timeout=2.0)
            logger.info("✅ Agent dispatch completed")
        except asyncio.TimeoutError:
            logger.warning("⚠️ Agent dispatch took longer than expected")
        
        logger.info("✅ Setup complete - monitoring for agent connection...")
        
        # Return the message task (the monitor runs independently)
        return message_task
    
    async def _setup_livekit(self):
        """Setup LiveKit connection and audio components"""
        # Setup event handlers
        event_handlers = {
            'on_connected': self._on_livekit_connected,
            'on_disconnected': self._on_livekit_disconnected,
            'on_participant_connected': self._on_participant_connected,
            'on_participant_disconnected': self._on_participant_disconnected,
            'on_track_published': self._on_track_published,
            'on_track_subscribed': self._on_track_subscribed,
            'on_track_unsubscribed': self._on_track_unsubscribed
        }
        
        # Connect to LiveKit
        success = await self.livekit_manager.connect_to_room(event_handlers)
        
        if success:
            # Create and publish audio track
            await self._setup_audio_track()
            logger.info(f"🎯 LiveKit connection complete - ready for audio!")
            logger.info(f"🎯 LiveKit ready in {time.time() - self.connection_start_time:.2f}s")
        
        return success
    
    async def _setup_audio_track(self):
        """Create and publish audio track"""
        self.audio_source = TelephonyAudioSource()
        self.audio_track = rtc.LocalAudioTrack.create_audio_track(
            "telephony-audio", 
            self.audio_source
        )
        
        publication = await self.livekit_manager.publish_audio_track(self.audio_track)
        if publication:
            logger.info(f"✅ Telephony audio track published: {publication.sid}")
    
    # LiveKit Event Handlers
    def _on_livekit_connected(self):
        """Handle LiveKit connection"""
        logger.info(f"✅ LiveKit connection established for room: {self.room_name}")
        room = self.livekit_manager.get_room()
        remote_participants = self.livekit_manager.get_remote_participants()
        logger.info(f"👥 Current participants in room: {len(remote_participants)}")
        
        # Check for existing participants
        for participant in remote_participants.values():
            logger.info(f"🔍 Found existing participant: {participant.identity}")
            self._handle_participant_joined(participant)
    
    def _on_livekit_disconnected(self):
        """Handle LiveKit disconnection"""
        logger.info(f"❌ LiveKit connection lost for room: {self.room_name}")
    
    def _on_participant_connected(self, participant):
        """Handle participant connection"""
        logger.info(f"👤 NEW participant joined: {participant.identity}")
        self._handle_participant_joined(participant)
    
    # def _on_participant_disconnected(self, participant):
    #     """Handle participant disconnection"""
    #     logger.info(f"👋 Participant left: {participant.identity}")
        
    #     # Clean up tracking
    #     if participant.identity in self.participants:
    #         del self.participants[participant.identity]
    #     if participant.identity in self.audio_tracks:
    #         del self.audio_tracks[participant.identity]
        
    #     if participant == self.agent_participant:
    #         logger.warning("🤖 AGENT PARTICIPANT DISCONNECTED!")
    #         self.agent_participant = None
    #         # Cancel audio streaming task
    #         if self.audio_stream_task and not self.audio_stream_task.done():
    #             logger.info("🔄 Cancelling audio stream task due to agent disconnect")
    #             self.audio_stream_task.cancel()
    
    def _on_participant_disconnected(self, participant):
        """Handle participant disconnection"""
        logger.info(f"👋 Participant left: {participant.identity}")
        
        # Clean up tracking
        if participant.identity in self.participants:
            del self.participants[participant.identity]
        if participant.identity in self.audio_tracks:
            del self.audio_tracks[participant.identity]
        
        if participant == self.agent_participant:
            logger.warning("🤖 AGENT PARTICIPANT DISCONNECTED!")
            self.agent_participant = None
            
            # 🆕 NEW: End user's call immediately when agent disconnects
            logger.info("📞 Agent disconnected - ending call for user immediately")
            asyncio.create_task(self._end_user_call_due_to_agent_disconnect())
            
            # Cancel audio streaming task
            if self.audio_stream_task and not self.audio_stream_task.done():
                logger.info("🔄 Cancelling audio stream task due to agent disconnect")
                self.audio_stream_task.cancel()


    async def _end_user_call_due_to_agent_disconnect(self):
        """End user's call when agent disconnects"""
        try:
            logger.info("🔚 Ending Plivo call due to agent disconnect")
            
            # Close the WebSocket connection to Plivo - this ends the user's call
            if hasattr(self, 'websocket') and self.websocket:
                try:
                    if not self.websocket.closed:
                        await self.websocket.close(code=1000, reason="Agent disconnected")
                        logger.info("✅ WebSocket closed - user call ended")
                    else:
                        logger.info("ℹ️ WebSocket already closed")
                except Exception as e:
                    logger.error(f"❌ Error closing WebSocket: {e}")
            
            # Trigger cleanup to ensure everything is cleaned up properly
            if not self.cleanup_started:
                logger.info("🧹 Triggering cleanup after agent disconnect")
                await self.cleanup()
                
        except Exception as e:
            logger.error(f"❌ Error ending user call due to agent disconnect: {e}")



    def _on_track_published(self, publication, participant):
        """Handle track publication"""
        logger.info(f"📡 Track PUBLISHED by {participant.identity}: {publication.kind}")
        
        if self.agent_manager.is_agent_participant(participant):
            logger.info(f"🤖 AGENT published {publication.kind} track")
    
    def _on_track_subscribed(self, track, publication, participant):
        """Handle track subscription"""
        logger.info(f"🎵 Track SUBSCRIBED from {participant.identity}: {track.kind}")
        
        # Store the track
        if participant.identity not in self.audio_tracks:
            self.audio_tracks[participant.identity] = []
        
        if track.kind == rtc.TrackKind.KIND_AUDIO:
            self.audio_tracks[participant.identity].append(track)
            logger.info(f"🔊 AUDIO TRACK STORED for {participant.identity}")
            
            # Check if this participant is the agent
            if self.agent_manager.is_agent_participant(participant):
                logger.info(f"🤖 AGENT AUDIO TRACK CONFIRMED! Starting stream to telephony...")
                self._start_agent_audio_stream(participant, track)
    
    def _on_track_unsubscribed(self, track, publication, participant):
        """Handle track unsubscription"""
        logger.info(f"🔇 Track unsubscribed from {participant.identity}: {track.kind}")
        
        # Remove from tracking
        if participant.identity in self.audio_tracks:
            if track in self.audio_tracks[participant.identity]:
                self.audio_tracks[participant.identity].remove(track)
    
    def _handle_participant_joined(self, participant):
        """Handle when a participant joins - ENHANCED WITH MONITOR NOTIFICATION"""
        logger.info(f"🔍 Analyzing participant: {participant.identity}")
        
        # Store participant
        self.participants[participant.identity] = participant
        
        # Check if this is an agent
        is_agent = self.agent_manager.log_agent_detection(participant)
        
        if is_agent:
            self.agent_participant = participant
            
            # CRITICAL: Notify the monitor that an agent connected
            if hasattr(self, 'agent_monitor') and self.agent_monitor:
                self.agent_monitor.notify_agent_connected()
                logger.info("📢 Notified monitor: Agent connected!")
            
            # Check if agent already has published audio tracks
            self._check_existing_agent_tracks(participant)
    
    def _check_existing_agent_tracks(self, participant):
        """Check if agent already has published tracks"""
        logger.info(f"🔍 Checking existing tracks for agent: {participant.identity}")
        
        agent_tracks = self.agent_manager.find_agent_audio_tracks(participant)
        for track in agent_tracks:
            self._start_agent_audio_stream(participant, track)
    
    def _start_agent_audio_stream(self, participant, track):
        """Start streaming agent audio to telephony"""
        # Don't start if cleanup has begun
        if self.cleanup_started or self.force_stop:
            logger.info("🛑 Not starting agent stream - cleanup in progress")
            return
            
        # Cancel existing stream task if any
        if self.audio_stream_task and not self.audio_stream_task.done():
            logger.info("🔄 Cancelling existing audio stream task")
            self.audio_stream_task.cancel()
        
        # Mark agent as speaking
        self.agent_is_speaking = True
        
        # Start new audio streaming task
        logger.info("🚀 Creating new audio stream task")
        self.audio_stream_task = asyncio.create_task(
            self._stream_agent_audio_to_telephony(track, participant.identity)
        )
    
    async def _stream_agent_audio_to_telephony(self, audio_track, participant_identity):
        """Stream agent's audio back to telephony system with background mixing"""
        logger.info(f"🔊 Starting agent audio stream to telephony from {participant_identity}")
        
        frame_count = 0
        last_log_time = time.time()
        bytes_sent = 0
        
        try:
            # Create audio stream
            audio_stream = rtc.AudioStream(audio_track)
            logger.info("✅ AudioStream created successfully")
            
            async for audio_frame_event in audio_stream:
                # CRITICAL: Check cleanup state first
                if self.cleanup_started or self.force_stop:
                    logger.info("🛑 Stopping agent audio stream - cleanup initiated")
                    break
                    
                current_time = time.time()
                
                # Check if still connected
                if not self._is_connection_active():
                    logger.warning("❌ Connection lost or call ended, stopping audio stream")
                    break
                
                frame_count += 1
                
                # Log every second
                if current_time - last_log_time >= 1.0:
                    logger.info(f"🔊 [OUTGOING] Agent audio: {frame_count} frames, {bytes_sent} bytes sent")
                    last_log_time = current_time
                
                try:
                    # Convert audio frame to clean telephony format (no background yet)
                    telephony_audio_data = self.audio_processor.convert_livekit_to_telephony(
                        audio_frame_event.frame
                    )
                    
                    # Send each audio chunk to telephony WITH background mixing
                    for clean_audio_chunk in telephony_audio_data:
                        # Check again before sending
                        if self.cleanup_started or self.force_stop:
                            logger.info("🛑 Stopping mid-frame - cleanup initiated")
                            break
                            
                        # Mix clean agent audio with background for user
                        mixed_audio_chunk = self.audio_processor.mix_agent_audio_with_background(
                            clean_audio_chunk
                        )
                        
                        success = await self.plivo_handler.send_audio_to_plivo(
                            self.websocket, mixed_audio_chunk
                        )
                        
                        if success:
                            bytes_sent += len(mixed_audio_chunk)
                            self.stats["audio_frames_received_from_agent"] += 1
                            self.stats["bytes_to_telephony"] += len(mixed_audio_chunk)
                        
                except Exception as e:
                    logger.error(f"❌ Error processing audio frame {frame_count}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"❌ Error in agent audio stream: {e}")
            import traceback
            traceback.print_exc()
        finally:
            # Mark agent as no longer speaking
            self.agent_is_speaking = False
            logger.info(f"🔇 Agent audio stream ended. Frames: {frame_count}, Bytes: {bytes_sent}")
    
    def _is_connection_active(self):
        """Check if connection is still active"""
        # If cleanup started, connection is not active
        if self.cleanup_started or self.force_stop:
            return False
            
        try:
            websocket_closed = (not hasattr(self.websocket, 'open') or 
                              not self.websocket.open if hasattr(self.websocket, 'open') else
                              getattr(self.websocket, 'closed', False))
        except:
            websocket_closed = True
        
        # Check all connection states
        livekit_connected = self.livekit_manager.is_connected()
        call_active = self.plivo_handler.is_call_active()
        
        is_active = livekit_connected and not websocket_closed and call_active
        
        return is_active
    
    async def _handle_messages(self):
        """Handle incoming WebSocket messages from Plivo"""
        logger.info(f"👂 Starting to listen for Plivo WebSocket messages...")
        
        try:
            async for message in self.websocket:
                # Check if cleanup started
                if self.cleanup_started or self.force_stop:
                    logger.info("🛑 Stopping message handling - cleanup initiated")
                    break
                    
                await self.plivo_handler.handle_message(
                    message,
                    audio_callback=self._handle_audio_from_plivo,
                    event_callback=self._handle_plivo_event
                )
                        
        except websockets.ConnectionClosed:
            logger.info("📞 Plivo WebSocket connection closed normally")
        except Exception as e:
            logger.error(f"❌ Error handling Plivo messages: {e}")
            import traceback
            traceback.print_exc()
        finally:
            # Ensure cleanup runs when message loop ends
            if not self.cleanup_started:
                logger.info("🔄 Message loop ended - starting cleanup")
                await self.cleanup()
    
    async def _handle_audio_from_plivo(self, audio_data):
        """Handle audio data from Plivo"""
        # Don't process if cleanup started
        if self.cleanup_started or self.force_stop:
            return
            
        if self.audio_source and self.livekit_manager.is_connected():
            if not self.audio_processor.validate_audio_data(audio_data):
                return
                
            try:
                await self.audio_source.push_audio_data(audio_data)
                self.stats["audio_frames_sent_to_livekit"] += 1
                self.stats["bytes_from_telephony"] += len(audio_data)
                
                # Log progress occasionally
                if self.stats["audio_frames_sent_to_livekit"] % 250 == 0:
                    logger.info(f"🎵 Processed {self.stats['audio_frames_sent_to_livekit']} audio frames from Plivo")
            except Exception as e:
                logger.error(f"❌ Error processing audio from Plivo: {e}")
        else:
            # Count dropped frames
            if not hasattr(self, 'dropped_frames'):
                self.dropped_frames = 0
            self.dropped_frames += 1
    
    async def _stream_background_audio_continuously(self):
        """Stream background audio continuously to user (never stops)"""
        if not self.audio_processor.get_noise_status()["enabled"]:
            logger.info("🔇 Background audio disabled, not starting continuous stream")
            return
        
        logger.info("🎵 Starting continuous background audio stream (always on)")
        
        frame_count = 0
        audio_frame_size = 80  # 10ms at 8kHz μ-law (80 bytes)
        
        try:
            while not self.cleanup_started and not self.force_stop and self._is_connection_active():
                # Always send background audio when agent is NOT speaking
                # When agent IS speaking, the mixed audio handles background
                if not self.agent_is_speaking:
                    # Get background audio chunk
                    bg_chunk = self.audio_processor.get_background_audio_chunk(audio_frame_size)
                    
                    if bg_chunk:
                        # Send pure background audio to user when agent is silent
                        success = await self.plivo_handler.send_audio_to_plivo(
                            self.websocket, bg_chunk
                        )
                        
                        if success:
                            frame_count += 1
                            self.stats["bytes_to_telephony"] += len(bg_chunk)
                
                # Send frames every 10ms to match audio timing
                await asyncio.sleep(0.01)  # 10ms intervals
                
        except Exception as e:
            logger.error(f"❌ Error in continuous background audio stream: {e}")
        finally:
            logger.info(f"🔇 Background audio stream ended. Background-only frames sent: {frame_count}")
    
    async def _handle_plivo_event(self, event_type):
        """Handle Plivo events"""
        if event_type == "call_ended":
            logger.info("🔴 Received call_ended event - triggering immediate cleanup")
            # Force cleanup immediately when call ends
            await self.cleanup()
    
    async def cleanup(self):
        """Enhanced cleanup with agent monitoring shutdown"""
        if self.cleanup_started:
            logger.info("🔄 Cleanup already in progress, skipping...")
            return
            
        self.cleanup_started = True
        self.force_stop = True
        
        logger.info("🧹 Starting ENHANCED cleanup with monitor shutdown...")
        
        # STEP 1: Stop agent monitoring FIRST
        if hasattr(self, 'agent_monitor') and self.agent_monitor:
            logger.info("🔄 Stopping agent connection monitor...")
            self.agent_monitor.stop_monitoring()
        
        # STEP 2: Immediately stop all audio processing
        logger.info("🛑 Setting force stop flags...")
        self.agent_is_speaking = False
        
        # STEP 3: Stop background audio processor immediately
        logger.info("🔇 Stopping background audio processor...")
        if self.audio_processor:
            self.audio_processor.stop()
        
        # STEP 4: Cancel background streaming task first (most persistent)
        if self.background_stream_task and not self.background_stream_task.done():
            logger.info("🔄 Cancelling background audio stream task...")
            self.background_stream_task.cancel()
            try:
                await asyncio.wait_for(self.background_stream_task, timeout=1.0)
                logger.info("✅ Background stream task cancelled quickly")
            except (asyncio.CancelledError, asyncio.TimeoutError):
                logger.info("⏰ Background stream task force cancelled")
        
        # STEP 5: Cancel agent audio streaming task
        if self.audio_stream_task and not self.audio_stream_task.done():
            logger.info("🔄 Cancelling agent audio stream task...")
            self.audio_stream_task.cancel()
            try:
                await asyncio.wait_for(self.audio_stream_task, timeout=1.0)
                logger.info("✅ Agent stream task cancelled quickly")
            except (asyncio.CancelledError, asyncio.TimeoutError):
                logger.info("⏰ Agent stream task force cancelled")
        
        # STEP 6: Disconnect from LiveKit IMMEDIATELY - this should signal the agent
        logger.info("🔗 Disconnecting from LiveKit to signal agent...")
        try:
            await asyncio.wait_for(self.livekit_manager.disconnect(), timeout=3.0)
            logger.info("✅ LiveKit disconnected - agent should now know call ended")
        except asyncio.TimeoutError:
            logger.warning("⏰ LiveKit disconnect timed out - but agent should still get signal")
        except Exception as e:
            logger.error(f"❌ Error disconnecting from LiveKit: {e}")
        
        # STEP 7: Cleanup audio components
        cleanup_tasks = []
        
        if self.audio_source:
            cleanup_tasks.append(self.audio_source.cleanup())
        if self.audio_processor:
            cleanup_tasks.append(self.audio_processor.cleanup())
        
        if cleanup_tasks:
            try:
                await asyncio.wait_for(asyncio.gather(*cleanup_tasks, return_exceptions=True), timeout=2.0)
                logger.info("✅ Audio components cleaned up")
            except asyncio.TimeoutError:
                logger.warning("⏰ Audio cleanup timed out")
        
        # STEP 8: Log final statistics
        await self._log_session_summary()
        
        logger.info("✅ ENHANCED cleanup complete - agent monitoring stopped, all audio stopped")
    
    async def _log_session_summary(self):
        """Log session summary statistics"""
        elapsed = time.time() - self.connection_start_time
        dropped_frames = getattr(self, 'dropped_frames', 0)
        plivo_stats = self.plivo_handler.get_call_stats()
        
        logger.info(f"📊 Session Summary:")
        logger.info(f"   Duration: {elapsed:.1f}s")
        logger.info(f"   Messages: {plivo_stats['messages_received']} received, {plivo_stats['messages_sent']} sent")
        logger.info(f"   Audio to LiveKit: {self.stats['audio_frames_sent_to_livekit']} frames, {self.stats['bytes_from_telephony']} bytes")
        logger.info(f"   Audio from Agent: {self.stats['audio_frames_received_from_agent']} frames, {self.stats['bytes_to_telephony']} bytes")
        logger.info(f"   Dropped frames (no LiveKit): {dropped_frames}")
        logger.info(f"   Agent: {'Found' if self.agent_participant else 'Not found'}")