"""
Agent management and detection for LiveKit rooms
"""
import asyncio
import subprocess
import logging
from livekit import rtc
from config import AGENT_NAME, AGENT_DISPATCH_TIMEOUT

logger = logging.getLogger(__name__)


class AgentManager:
    """Manages AI agents in LiveKit rooms"""
    
    def __init__(self):
        self.agent_patterns = [
            ("agent-", "identity starts with 'agent-'"),
            (AGENT_NAME.lower(), f"identity contains '{AGENT_NAME}'"),
            ("ac_", "identity starts with 'AC_'"),
            ("agent", "identity contains 'agent'"),
            ("assistant", "identity contains 'assistant'"),
            ("ai-", "identity starts with 'ai-'"),
        ]
    
    async def trigger_agent(self, room_name, agent_name=None):
        """Launch agent in the specified LiveKit room"""
        if agent_name is None:
            agent_name = AGENT_NAME
            
        logger.info(f"🚀 Triggering agent '{agent_name}' for room: {room_name}")
        try:
            logger.info(f"🎯 Dispatching agent to room: {room_name}")
            
            # Use Popen for non-blocking execution
            result = subprocess.Popen([
                "lk", "dispatch", "create",
                "--room", room_name,
                "--agent-name", agent_name
            ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            logger.info(f"✅ Agent '{agent_name}' dispatch command executed immediately (PID: {result.pid})")
            
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
    
    def is_agent_participant(self, participant, return_reasons=False):
        """Check if participant is an agent"""
        reasons = []
        is_agent = False
        
        identity = participant.identity.lower()
        
        for pattern, reason in self.agent_patterns:
            if pattern in identity:
                is_agent = True
                reasons.append(reason)
        
        if return_reasons:
            return is_agent, reasons
        return is_agent
    
    def is_agent_participant_identity(self, identity):
        """Check if identity string belongs to an agent"""
        identity = identity.lower()
        return any(pattern[0] in identity for pattern in self.agent_patterns)
    
    def find_agent_audio_tracks(self, participant):
        """Find existing audio tracks for an agent participant"""
        agent_tracks = []
        
        if not self.is_agent_participant(participant):
            return agent_tracks
        
        track_publications = list(participant.track_publications.values())
        logger.info(f"🔍 Agent has {len(track_publications)} published tracks")
        
        for publication in track_publications:
            if (publication.kind == rtc.TrackKind.KIND_AUDIO and 
                publication.subscribed and publication.track):
                agent_tracks.append(publication.track)
                logger.info(f"🤖 Found existing AGENT AUDIO TRACK!")
        
        return agent_tracks
    
    def log_agent_detection(self, participant):
        """Log agent detection with reasons"""
        is_agent, reasons = self.is_agent_participant(participant, return_reasons=True)
        
        if is_agent:
            logger.info(f"🤖 AGENT DETECTED: {participant.identity}")
            logger.info(f"🤖 Detection reasons: {', '.join(reasons)}")
        else:
            logger.info(f"👥 Regular participant: {participant.identity}")
        
        return is_agent


