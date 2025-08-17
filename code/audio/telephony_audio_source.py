"""
Telephony Audio Source for processing μ-law audio data
"""
import time
import array
import audioop
import logging
from livekit import rtc
from config import TELEPHONY_SAMPLE_RATE, LIVEKIT_SAMPLE_RATE, AUDIO_LOG_FREQUENCY

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
            if self.frame_count % AUDIO_LOG_FREQUENCY == 0:
                logger.info(f"🎵 [INCOMING] Frame #{self.frame_count}: {len(mulaw_data)} bytes μ-law, "
                           f"Total: {self.total_bytes_processed} bytes")

            # Convert μ-law to 16-bit PCM
            pcm_data = self._convert_mulaw_to_pcm(mulaw_data)
            if not pcm_data:
                return
            
            # Convert to samples array
            samples = self._pcm_to_samples(pcm_data)
            if not samples:
                return

            # Create and resample audio frame
            resampled_frames = self._resample_audio(samples)
            
            # Push each resampled frame to LiveKit
            await self._push_resampled_frames(resampled_frames)

        except Exception as e:
            logger.error(f"❌ Error processing telephony audio frame {self.frame_count}: {e}")
            import traceback
            traceback.print_exc()

    def _convert_mulaw_to_pcm(self, mulaw_data):
        """Convert μ-law to 16-bit PCM"""
        try:
            return audioop.ulaw2lin(mulaw_data, 2)  # 2 bytes per sample (16-bit)
        except Exception as e:
            logger.error(f"❌ μ-law conversion error: {e}, data size: {len(mulaw_data)}")
            return None

    def _pcm_to_samples(self, pcm_data):
        """Convert PCM data to samples array"""
        try:
            samples = array.array("h")  # signed short (16-bit)
            samples.frombytes(pcm_data)
            
            if len(samples) == 0:
                logger.warning("⚠️ No samples after PCM conversion")
                return None
                
            # Log sample info for first few frames only
            if self.frame_count <= 5:
                logger.info(f"🔍 Frame {self.frame_count}: {len(samples)} samples, "
                           f"first few: {samples[:min(5, len(samples))]}")
            
            return samples
        except Exception as e:
            logger.error(f"❌ Error converting PCM to samples: {e}")
            return None

    def _resample_audio(self, samples):
        """Resample audio to LiveKit's sample rate"""
        try:
            # Create input frame for resampling
            input_frame = rtc.AudioFrame.create(
                sample_rate=TELEPHONY_SAMPLE_RATE,
                num_channels=1,
                samples_per_channel=len(samples)
            )
            input_frame.data[:len(samples)] = samples

            # Resample to LiveKit's sample rate
            return self.resampler.push(input_frame)
        except Exception as e:
            logger.error(f"❌ Error resampling audio: {e}")
            return []

    async def _push_resampled_frames(self, resampled_frames):
        """Push resampled frames to LiveKit"""
        for i, resampled_frame in enumerate(resampled_frames):
            await self.capture_frame(resampled_frame)
            
            if self.frame_count <= 5:
                logger.info(f"🔍 Pushed resampled frame {i}: {resampled_frame.samples_per_channel} samples")

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