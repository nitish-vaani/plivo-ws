"""
Audio processing utilities for telephony-LiveKit bridge - FIXED VERSION
"""
import audioop
import numpy as np
import logging
from livekit import rtc
from config import TELEPHONY_SAMPLE_RATE, LIVEKIT_SAMPLE_RATE
from audio.noise_manager import NoiseManager

logger = logging.getLogger(__name__)


class AudioProcessor:
    """Handles audio conversion and processing between telephony and LiveKit - FIXED VERSION"""
    
    def __init__(self):
        self.return_resampler = rtc.AudioResampler(
            input_rate=LIVEKIT_SAMPLE_RATE,
            output_rate=TELEPHONY_SAMPLE_RATE,
            num_channels=1,
            quality=rtc.AudioResamplerQuality.HIGH
        )
        self.noise_manager = NoiseManager()
        self.is_active = True  # Add active state tracking
        
    def create_return_resampler(self):
        """Create resampler for return audio path (LiveKit -> Telephony)"""
        return rtc.AudioResampler(
            input_rate=LIVEKIT_SAMPLE_RATE,
            output_rate=TELEPHONY_SAMPLE_RATE,
            num_channels=1,
            quality=rtc.AudioResamplerQuality.HIGH
        )
    
    def convert_livekit_to_telephony(self, audio_frame):
        """Convert LiveKit audio frame to telephony μ-law format WITHOUT background noise"""
        if not self.is_active:
            return []
            
        try:
            # Resample from 48kHz to 8kHz for telephony
            resampled_frames = self.return_resampler.push(audio_frame)
            
            telephony_audio_data = []
            
            for resampled_frame in resampled_frames:
                if not self.is_active:  # Check again during processing
                    break
                    
                # Convert to PCM bytes
                pcm_bytes = bytes(resampled_frame.data[:resampled_frame.samples_per_channel * 2])
                
                # Add subtle noise to improve audio quality
                noisy_pcm = self._add_audio_noise(pcm_bytes)
                
                # Convert PCM to μ-law for telephony
                mulaw_bytes = audioop.lin2ulaw(noisy_pcm, 2)
                
                # NO background noise here - this is clean agent audio
                telephony_audio_data.append(mulaw_bytes)
                
            return telephony_audio_data
            
        except Exception as e:
            logger.error(f"❌ Error converting LiveKit audio to telephony: {e}")
            return []
    
    def mix_agent_audio_with_background(self, agent_audio_data):
        """Mix clean agent audio with background noise for user"""
        if not self.is_active or not self.noise_manager.enabled:
            return agent_audio_data
        
        try:
            # Apply background noise mixing at μ-law level
            mixed_audio = self.noise_manager.apply_noise_to_frame(
                agent_audio_data, 
                len(agent_audio_data)
            )
            return mixed_audio
        except Exception as e:
            logger.error(f"❌ Error mixing agent audio with background: {e}")
            return agent_audio_data
    
    def _add_audio_noise(self, pcm_bytes):
        """Add subtle noise to PCM audio to improve quality"""
        if not self.is_active:
            return pcm_bytes
            
        try:
            pcm_array = np.frombuffer(pcm_bytes, dtype=np.int16)
            noise = np.random.normal(0, 0.02, len(pcm_array))
            noisy_pcm = (pcm_array + noise * 32767 * 0.1).astype(np.int16)
            return noisy_pcm.tobytes()
        except Exception as e:
            logger.error(f"❌ Error adding audio noise: {e}")
            return pcm_bytes
    
    def validate_audio_data(self, audio_data):
        """Validate audio data before processing"""
        if not self.is_active:
            return False
            
        if not audio_data:
            logger.warning("⚠️ Empty audio data received")
            return False
            
        if len(audio_data) == 0:
            logger.warning("⚠️ Zero-length audio data received")
            return False
            
        return True
    
    def stop(self):
        """Stop audio processor immediately"""
        logger.info("🛑 Stopping audio processor...")
        self.is_active = False
        
        # Stop noise manager
        if self.noise_manager:
            self.noise_manager.stop()
        
        logger.info("✅ Audio processor stopped")
    
    async def cleanup(self):
        """Clean up audio processor resources"""
        try:
            # Stop first
            self.stop()
            
            # Clean up resampler
            if hasattr(self.return_resampler, 'aclose'):
                await self.return_resampler.aclose()
            elif hasattr(self.return_resampler, 'close'):
                self.return_resampler.close()
                
            logger.info("✅ Audio processor cleanup complete")
        except Exception as e:
            logger.error(f"❌ Error cleaning up audio processor: {e}")
    
    def update_noise_settings(self, **kwargs):
        """Update background noise settings"""
        if self.noise_manager:
            self.noise_manager.update_settings(**kwargs)
    
    def get_background_audio_chunk(self, chunk_size):
        """Get background audio chunk for continuous streaming"""
        if not self.is_active or not self.noise_manager:
            return None
        return self.noise_manager.get_background_chunk(chunk_size)
    
    def start_background_audio(self):
        """Start background audio streaming"""
        if self.noise_manager:
            self.noise_manager.start()
    
    def get_noise_status(self):
        """Get noise manager status"""
        if self.noise_manager:
            status = self.noise_manager.get_status()
            status["processor_active"] = self.is_active
            return status
        return {
            "enabled": False,
            "processor_active": self.is_active
        }