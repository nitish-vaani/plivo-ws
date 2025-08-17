"""
Background noise manager using FFmpeg for robust audio processing
"""
import os
import asyncio
import logging
import numpy as np
import subprocess
import tempfile
import wave
import audioop
import threading
import time
from pathlib import Path
from collections import deque
from config import (
    BG_NOISE_ENABLED, NOISE_TYPE, NOISE_VOLUME, NOISE_FOLDER,
    TELEPHONY_SAMPLE_RATE
)

logger = logging.getLogger(__name__)


class NoiseManager:
    """Manages background noise using FFmpeg for audio processing"""
    
    def __init__(self):
        self.enabled = BG_NOISE_ENABLED
        self.noise_type = NOISE_TYPE
        self.volume = NOISE_VOLUME
        self.noise_data = None
        self.current_position = 0
        self.is_running = False
        self.lock = threading.Lock()
        
        if self.enabled:
            self._load_noise_file()
    
    def _load_noise_file(self):
        """Load noise file using FFmpeg for robust audio processing"""
        try:
            noise_file = Path(NOISE_FOLDER) / f"{self.noise_type}.mp3"
            
            if not noise_file.exists():
                logger.error(f"❌ Noise file not found: {noise_file}")
                self.enabled = False
                return
            
            logger.info(f"🔊 Loading noise file with FFmpeg: {noise_file}")
            
            # Use FFmpeg to convert to telephony format
            with tempfile.NamedTemporaryFile(suffix='.wav', delete=False) as temp_file:
                temp_path = temp_file.name
            
            try:
                # Convert to 8kHz mono PCM using FFmpeg
                result = subprocess.run([
                    'ffmpeg', '-i', str(noise_file),
                    '-ar', str(TELEPHONY_SAMPLE_RATE),
                    '-ac', '1',
                    '-f', 'wav',
                    '-y',  # Overwrite output file
                    temp_path
                ], capture_output=True, text=True, timeout=30)
                
                if result.returncode != 0:
                    logger.error(f"❌ FFmpeg conversion failed: {result.stderr}")
                    self.enabled = False
                    return
                
                # Read the converted WAV file
                with wave.open(temp_path, 'rb') as wav_file:
                    if wav_file.getnchannels() != 1:
                        logger.error("❌ Background audio must be mono")
                        self.enabled = False
                        return
                    
                    if wav_file.getframerate() != TELEPHONY_SAMPLE_RATE:
                        logger.error(f"❌ Background audio must be {TELEPHONY_SAMPLE_RATE}Hz")
                        self.enabled = False
                        return
                    
                    # Read PCM data
                    pcm_data = wav_file.readframes(wav_file.getnframes())
                    
                    # Convert PCM to μ-law for telephony
                    self.noise_data = audioop.lin2ulaw(pcm_data, 2)
                    
                    duration = len(pcm_data) / (2 * TELEPHONY_SAMPLE_RATE)  # 2 bytes per sample
                    logger.info(f"✅ Background noise loaded: {len(self.noise_data)} bytes, {duration:.1f}s")
                
            finally:
                # Clean up temp file
                try:
                    os.unlink(temp_path)
                except:
                    pass
                    
        except subprocess.TimeoutExpired:
            logger.error("❌ FFmpeg conversion timed out")
            self.enabled = False
        except FileNotFoundError:
            logger.error("❌ FFmpeg not found. Please install FFmpeg")
            logger.error("   Ubuntu/Debian: sudo apt install ffmpeg")
            logger.error("   macOS: brew install ffmpeg")
            self.enabled = False
        except Exception as e:
            logger.error(f"❌ Error loading noise file: {e}")
            self.enabled = False
    
    def apply_noise_to_frame(self, audio_frame_data, num_samples):
        """Apply background noise to audio frame using μ-law mixing"""
        if not self.enabled or self.noise_data is None:
            return audio_frame_data
        
        try:
            with self.lock:
                # Get noise samples for this frame
                noise_chunk = self._get_noise_chunk(len(audio_frame_data))
                
                if noise_chunk:
                    # Mix at μ-law level for efficiency
                    mixed_audio = self._mix_mulaw_audio(audio_frame_data, noise_chunk)
                    return mixed_audio
                
                return audio_frame_data
                
        except Exception as e:
            logger.error(f"❌ Error applying noise: {e}")
            return audio_frame_data
    
    def _get_noise_chunk(self, chunk_size):
        """Get noise chunk with looping"""
        if not self.noise_data:
            return None
        
        data_len = len(self.noise_data)
        
        if self.current_position >= data_len:
            self.current_position = 0  # Loop back to start
        
        # Get chunk, handle wrap-around
        if self.current_position + chunk_size <= data_len:
            chunk = self.noise_data[self.current_position:self.current_position + chunk_size]
            self.current_position += chunk_size
        else:
            # Need to wrap around
            first_part = self.noise_data[self.current_position:]
            remaining = chunk_size - len(first_part)
            second_part = self.noise_data[:remaining]
            chunk = first_part + second_part
            self.current_position = remaining
        
        return chunk
    
    def _mix_mulaw_audio(self, agent_mulaw, noise_mulaw):
        """Mix agent audio with background noise at μ-law level"""
        try:
            if not agent_mulaw or not noise_mulaw:
                return agent_mulaw or noise_mulaw
            
            # Convert both to PCM for mixing
            agent_pcm = audioop.ulaw2lin(agent_mulaw, 2)
            noise_pcm = audioop.ulaw2lin(noise_mulaw, 2)
            
            # Ensure same length
            agent_len = len(agent_pcm)
            noise_len = len(noise_pcm)
            
            if noise_len < agent_len:
                # Repeat noise if too short
                repetitions = (agent_len // noise_len) + 1
                noise_pcm = (noise_pcm * repetitions)[:agent_len]
            elif noise_len > agent_len:
                # Truncate noise if too long
                noise_pcm = noise_pcm[:agent_len]
            
            # Convert to sample arrays for mixing
            import array
            agent_samples = array.array('h')
            noise_samples = array.array('h')
            
            agent_samples.frombytes(agent_pcm)
            noise_samples.frombytes(noise_pcm)
            
            # Mix samples with volume control
            mixed_samples = array.array('h')
            for i in range(len(agent_samples)):
                # Mix: agent at full volume + noise at reduced volume
                agent_sample = agent_samples[i]
                noise_sample = int(noise_samples[i] * self.volume)
                
                # Prevent clipping
                mixed_sample = agent_sample + noise_sample
                mixed_sample = max(-32768, min(32767, mixed_sample))
                mixed_samples.append(mixed_sample)
            
            # Convert back to μ-law
            mixed_pcm = mixed_samples.tobytes()
            mixed_mulaw = audioop.lin2ulaw(mixed_pcm, 2)
            
            return mixed_mulaw
            
        except Exception as e:
            logger.error(f"❌ Error mixing audio: {e}")
            return agent_mulaw  # Return agent audio if mixing fails
    
    def get_background_chunk(self, chunk_size):
        """Get a chunk of background audio for continuous streaming"""
        if not self.enabled or not self.noise_data:
            return None
        
        with self.lock:
            return self._get_noise_chunk(chunk_size)
    
    def update_settings(self, noise_type=None, volume=None, enabled=None):
        """Update noise settings dynamically"""
        if enabled is not None:
            self.enabled = enabled
        
        if volume is not None:
            self.volume = max(0.0, min(1.0, volume))  # Clamp between 0 and 1
            logger.info(f"🔊 Updated noise volume: {self.volume}")
        
        if noise_type is not None and noise_type != self.noise_type:
            self.noise_type = noise_type
            logger.info(f"🔊 Switching to noise type: {noise_type}")
            if self.enabled:
                self._load_noise_file()
    
    def start(self):
        """Start background noise"""
        self.is_running = True
        logger.info("🎵 Background noise started")
    
    def stop(self):
        """Stop background noise"""
        self.is_running = False
        logger.info("🔇 Background noise stopped")
    
    def get_status(self):
        """Get current noise manager status"""
        return {
            "enabled": self.enabled,
            "noise_type": self.noise_type,
            "volume": self.volume,
            "ffmpeg_available": self._check_ffmpeg(),
            "noise_loaded": self.noise_data is not None,
            "noise_samples": len(self.noise_data) if self.noise_data is not None else 0,
            "is_running": self.is_running
        }
    
    def _check_ffmpeg(self):
        """Check if FFmpeg is available"""
        try:
            result = subprocess.run(['ffmpeg', '-version'], 
                                  capture_output=True, timeout=5)
            return result.returncode == 0
        except:
            return False