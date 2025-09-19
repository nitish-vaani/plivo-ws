# """
# Dynamic agent connection timeout monitor
# """
# import asyncio
# import time
# import logging
# import json

# logger = logging.getLogger(__name__)


# class AgentConnectionMonitor:
#     """Monitors agent connection and drops call if agent doesn't connect within timeout"""
    
#     def __init__(self, websocket_handler, timeout_seconds=5):
#         self.websocket_handler = websocket_handler
#         self.timeout_seconds = timeout_seconds
#         self.agent_connected = False
#         self.monitoring_task = None
#         self.timeout_reached = False
#         self.monitoring_active = True
        
#     async def start_monitoring(self):
#         """Start monitoring for agent connection"""
#         logger.info(f"⏰ Starting agent connection monitor - {self.timeout_seconds}s timeout")
        
#         # Start the timeout task
#         self.monitoring_task = asyncio.create_task(self._monitor_agent_connection())
        
#         return self.monitoring_task
    
#     async def _monitor_agent_connection(self):
#         """Monitor for agent connection with timeout"""
#         start_time = time.time()
        
#         try:
#             while time.time() - start_time < self.timeout_seconds and self.monitoring_active:
#                 # Check if agent has connected
#                 if self.websocket_handler.agent_participant is not None:
#                     self.agent_connected = True
#                     elapsed = time.time() - start_time
#                     logger.info(f"✅ Agent connected in {elapsed:.2f}s - call will continue")
#                     return True
                
#                 # Check every 0.1 seconds
#                 await asyncio.sleep(0.1)
            
#             # Only drop call if monitoring is still active (not cancelled due to cleanup)
#             if self.monitoring_active:
#                 # Timeout reached
#                 self.timeout_reached = True
#                 elapsed = time.time() - start_time
#                 logger.warning(f"⏰ Agent connection timeout after {elapsed:.2f}s - dropping call")
                
#                 # Drop the call
#                 await self._drop_call_no_agent()
#                 return False
            
#             return self.agent_connected
            
#         except asyncio.CancelledError:
#             logger.info("🔄 Agent connection monitoring cancelled")
#             return self.agent_connected
#         except Exception as e:
#             logger.error(f"❌ Error in agent connection monitoring: {e}")
#             return False
    
#     # async def _drop_call_no_agent(self):
#     #     """Drop the call because no agent connected"""
#     #     logger.info("📞 Dropping call - no agent available")
        
#     #     try:
#     #         # Send a message to Plivo that no agent is available (optional)
#     #         if (hasattr(self.websocket_handler, 'websocket') and 
#     #             hasattr(self.websocket_handler.websocket, 'send')):
#     #             try:
#     #                 logger.info("📢 Notifying caller that no agent is available")
#     #                 # You could implement a brief audio message here if needed
                    
#     #             except Exception as e:
#     #                 logger.error(f"❌ Error sending no-agent message: {e}")
            
#     #         # Close the WebSocket connection cleanly
#     #         if hasattr(self.websocket_handler, 'websocket'):
#     #             try:
#     #                 await self.websocket_handler.websocket.close(
#     #                     code=1000, 
#     #                     reason="No agent available"
#     #                 )
#     #                 logger.info("✅ WebSocket closed - no agent available")
#     #             except Exception as e:
#     #                 logger.error(f"❌ Error closing WebSocket: {e}")
            
#     #         # Trigger cleanup of the handler
#     #         if hasattr(self.websocket_handler, 'cleanup'):
#     #             logger.info("🧹 Triggering handler cleanup due to no agent")
#     #             await self.websocket_handler.cleanup()
                
#     #     except Exception as e:
#     #         logger.error(f"❌ Error dropping call: {e}")
    
#     async def _drop_call_no_agent(self):
#         """Drop the call because no agent connected"""
#         logger.info("📞 Agent connection timeout - ending call for user immediately")
        
#         try:
#             # Close the WebSocket connection cleanly - this ends user's call
#             if (hasattr(self.websocket_handler, 'websocket') and 
#                 self.websocket_handler.websocket):
#                 try:
#                     if not self.websocket_handler.websocket.closed:
#                         await self.websocket_handler.websocket.close(
#                             code=1000, 
#                             reason="No agent available - timeout"
#                         )
#                         logger.info("✅ WebSocket closed - user call ended due to agent timeout")
#                     else:
#                         logger.info("ℹ️ WebSocket already closed")
#                 except Exception as e:
#                     logger.error(f"❌ Error closing WebSocket: {e}")
            
#             # Trigger cleanup of the handler
#             if hasattr(self.websocket_handler, 'cleanup'):
#                 logger.info("🧹 Triggering handler cleanup due to agent timeout")
#                 if not self.websocket_handler.cleanup_started:
#                     await self.websocket_handler.cleanup()
                    
#         except Exception as e:
#             logger.error(f"❌ Error dropping call due to agent timeout: {e}")


#     def notify_agent_connected(self):
#         """Called when an agent connects"""
#         if not self.timeout_reached and self.monitoring_active:
#             self.agent_connected = True
#             logger.info("🤖 Agent connection confirmed by monitor")
    
#     def stop_monitoring(self):
#         """Stop the monitoring task"""
#         self.monitoring_active = False
#         if self.monitoring_task and not self.monitoring_task.done():
#             logger.info("🔄 Stopping agent connection monitor...")
#             self.monitoring_task.cancel()





"""
Enhanced agent connection timeout monitor with better call termination
"""
import asyncio
import time
import logging

logger = logging.getLogger(__name__)


class AgentConnectionMonitor:
    """Enhanced monitor with immediate call termination capabilities"""
    
    def __init__(self, websocket_handler, timeout_seconds=5):
        self.websocket_handler = websocket_handler
        self.timeout_seconds = timeout_seconds
        self.agent_connected = False
        self.monitoring_task = None
        self.timeout_reached = False
        self.monitoring_active = True
        
    async def start_monitoring(self):
        """Start monitoring for agent connection"""
        logger.info(f"⏰ Starting agent connection monitor - {self.timeout_seconds}s timeout")
        
        self.monitoring_task = asyncio.create_task(self._monitor_agent_connection())
        return self.monitoring_task
    
    async def _monitor_agent_connection(self):
        """Monitor for agent connection with timeout"""
        start_time = time.time()
        
        try:
            while time.time() - start_time < self.timeout_seconds and self.monitoring_active:
                # Check if agent has connected
                if self.websocket_handler.agent_participant is not None:
                    self.agent_connected = True
                    elapsed = time.time() - start_time
                    logger.info(f"✅ Agent connected in {elapsed:.2f}s - call will continue")
                    return True
                
                # Check every 0.1 seconds
                await asyncio.sleep(0.1)
            
            # Only drop call if monitoring is still active
            if self.monitoring_active:
                self.timeout_reached = True
                elapsed = time.time() - start_time
                logger.warning(f"⏰ Agent connection timeout after {elapsed:.2f}s - terminating call")
                
                # Use the enhanced termination method
                await self._terminate_call_no_agent()
                return False
            
            return self.agent_connected
            
        except asyncio.CancelledError:
            logger.info("🔄 Agent connection monitoring cancelled")
            return self.agent_connected
        except Exception as e:
            logger.error(f"❌ Error in agent connection monitoring: {e}")
            return False
    
    async def _terminate_call_no_agent(self):
        """Terminate call immediately when no agent connects - ENHANCED"""
        logger.warning("📞 Agent connection timeout - terminating telephony call immediately")
        
        try:
            # Mark the call as ended in the handler
            if hasattr(self.websocket_handler, 'call_ended'):
                self.websocket_handler.call_ended = True
                self.websocket_handler.call_termination_reason = "agent_timeout"
            
            # Use the handler's enhanced termination method if available
            if hasattr(self.websocket_handler, '_terminate_call_immediately'):
                await self.websocket_handler._terminate_call_immediately("Agent connection timeout")
            else:
                # Fallback to direct WebSocket close
                await self._fallback_call_termination()
                    
        except Exception as e:
            logger.error(f"❌ Error terminating call due to agent timeout: {e}")
            # Force cleanup anyway
            if hasattr(self.websocket_handler, 'cleanup'):
                await self.websocket_handler.cleanup()

    async def _fallback_call_termination(self):
        """Fallback method for call termination"""
        logger.warning("🔄 Using fallback call termination method")
        
        try:
            # Close WebSocket directly
            if (hasattr(self.websocket_handler, 'websocket') and 
                self.websocket_handler.websocket):
                
                if not self.websocket_handler.websocket.closed:
                    await self.websocket_handler.websocket.close(
                        code=1000, 
                        reason="Agent connection timeout"
                    )
                    logger.warning("✅ WebSocket closed via fallback - user call ended")
            
            # Trigger cleanup
            if (hasattr(self.websocket_handler, 'cleanup') and 
                not getattr(self.websocket_handler, 'cleanup_started', False)):
                await self.websocket_handler.cleanup()
                
        except Exception as e:
            logger.error(f"❌ Error in fallback call termination: {e}")

    def notify_agent_connected(self):
        """Called when an agent connects"""
        if not self.timeout_reached and self.monitoring_active:
            self.agent_connected = True
            logger.info("🤖 Agent connection confirmed by monitor")
    
    def stop_monitoring(self):
        """Stop the monitoring task"""
        self.monitoring_active = False
        if self.monitoring_task and not self.monitoring_task.done():
            logger.info("🔄 Stopping agent connection monitor...")
            self.monitoring_task.cancel()



