"""
Persistent storage for Raft state.
Handles saving and loading state from disk with atomic operations.
"""

import json
import os
import threading
import time
from typing import Dict, Optional, List
from common.logger import get_logger

logger = get_logger(__name__)


class RaftStorage:
    """
    Manages persistent storage for Raft state.
    Ensures durability and atomicity of state updates.
    """
    
    def __init__(self, node_id: str, storage_dir: str):
        self.node_id = node_id
        self.storage_dir = storage_dir
        self.state_file = os.path.join(storage_dir, f"{node_id}_state.json")
        self.snapshot_dir = os.path.join(storage_dir, "snapshots")
        self.lock = threading.RLock()
        
        # Create directories if they don't exist
        os.makedirs(storage_dir, exist_ok=True)
        os.makedirs(self.snapshot_dir, exist_ok=True)
    
    def save_state(self, current_term: int, voted_for: Optional[str]):
        """
        Save the persistent state (currentTerm and votedFor).
        Must be called before responding to RPCs.
        """
        with self.lock:
            try:
                data = {
                    'current_term': current_term,
                    'voted_for': voted_for
                }
                
                # Write to temporary file first
                temp_file = self.state_file + '.tmp'
                with open(temp_file, 'w') as f:
                    json.dump(data, f, indent=2)
                
                # Atomically rename to ensure consistency
                os.replace(temp_file, self.state_file)
                
                logger.debug(f"Saved state: term={current_term}, voted_for={voted_for}")
                
            except Exception as e:
                logger.error(f"Failed to save state: {e}")
                raise
    
    def load_state(self) -> Dict:
        """
        Load the persistent state from disk.
        Returns dict with 'current_term' and 'voted_for'.
        """
        with self.lock:
            try:
                if os.path.exists(self.state_file):
                    with open(self.state_file, 'r') as f:
                        data = json.load(f)
                    
                    logger.info(f"Loaded state: term={data.get('current_term')}, "
                               f"voted_for={data.get('voted_for')}")
                    return data
                else:
                    # No existing state, return defaults
                    return {
                        'current_term': 0,
                        'voted_for': None
                    }
                    
            except Exception as e:
                logger.error(f"Failed to load state: {e}")
                # Return defaults on error
                return {
                    'current_term': 0,
                    'voted_for': None
                }
    
    def save_snapshot(self, snapshot_data: Dict, last_included_index: int, 
                     last_included_term: int, configuration: List[str]):
        """
        Save a snapshot of the state machine.
        Used for log compaction.
        """
        with self.lock:
            try:
                snapshot_file = os.path.join(
                    self.snapshot_dir, 
                    f"{self.node_id}_snapshot_{last_included_index}.json"
                )
                
                data = {
                    'last_included_index': last_included_index,
                    'last_included_term': last_included_term,
                    'configuration': configuration,
                    'state_machine_data': snapshot_data
                }
                
                # Write to temporary file first
                temp_file = snapshot_file + '.tmp'
                with open(temp_file, 'w') as f:
                    json.dump(data, f, indent=2)
                
                # Atomically rename
                os.replace(temp_file, snapshot_file)
                
                # Clean up old snapshots (keep only the latest 3)
                self._cleanup_old_snapshots(last_included_index)
                
                logger.info(f"Saved snapshot at index {last_included_index}")
                
            except Exception as e:
                logger.error(f"Failed to save snapshot: {e}")
                raise
    
    def load_latest_snapshot(self) -> Optional[Dict]:
        """
        Load the latest snapshot if one exists.
        Returns dict with snapshot data or None.
        """
        with self.lock:
            try:
                # Find all snapshot files
                snapshot_files = []
                for filename in os.listdir(self.snapshot_dir):
                    if filename.startswith(f"{self.node_id}_snapshot_") and filename.endswith('.json'):
                        try:
                            # Extract index from filename
                            index_str = filename.replace(f"{self.node_id}_snapshot_", '').replace('.json', '')
                            index = int(index_str)
                            snapshot_files.append((index, filename))
                        except ValueError:
                            continue
                
                if not snapshot_files:
                    return None
                
                # Sort by index and get the latest
                snapshot_files.sort(key=lambda x: x[0], reverse=True)
                latest_index, latest_file = snapshot_files[0]
                
                # Load the snapshot
                snapshot_path = os.path.join(self.snapshot_dir, latest_file)
                with open(snapshot_path, 'r') as f:
                    data = json.load(f)
                
                logger.info(f"Loaded snapshot at index {data['last_included_index']}")
                return data
                
            except Exception as e:
                logger.error(f"Failed to load snapshot: {e}")
                return None
    
    def save_configuration(self, configuration: List[str]):
        """
        Save the current cluster configuration.
        Used for dynamic membership changes.
        """
        with self.lock:
            try:
                config_file = os.path.join(self.storage_dir, f"{self.node_id}_config.json")
                
                data = {
                    'configuration': configuration,
                    'timestamp': os.path.getmtime(config_file) if os.path.exists(config_file) else 0
                }
                
                # Write to temporary file first
                temp_file = config_file + '.tmp'
                with open(temp_file, 'w') as f:
                    json.dump(data, f, indent=2)
                
                # Atomically rename
                os.replace(temp_file, config_file)
                
                logger.info(f"Saved configuration: {configuration}")
                
            except Exception as e:
                logger.error(f"Failed to save configuration: {e}")
                raise
    
    def load_configuration(self) -> List[str]:
        """
        Load the cluster configuration.
        Returns list of node IDs or empty list if none exists.
        """
        with self.lock:
            try:
                config_file = os.path.join(self.storage_dir, f"{self.node_id}_config.json")
                
                if os.path.exists(config_file):
                    with open(config_file, 'r') as f:
                        data = json.load(f)
                    return data.get('configuration', [])
                else:
                    return []
                    
            except Exception as e:
                logger.error(f"Failed to load configuration: {e}")
                return []
    
    def _cleanup_old_snapshots(self, current_index: int):
        """Keep only the latest 3 snapshots to save disk space"""
        try:
            # Find all snapshot files
            snapshot_files = []
            for filename in os.listdir(self.snapshot_dir):
                if filename.startswith(f"{self.node_id}_snapshot_") and filename.endswith('.json'):
                    try:
                        index_str = filename.replace(f"{self.node_id}_snapshot_", '').replace('.json', '')
                        index = int(index_str)
                        snapshot_files.append((index, filename))
                    except ValueError:
                        continue
            
            # Sort by index in descending order
            snapshot_files.sort(key=lambda x: x[0], reverse=True)
            
            # Keep only the latest 3
            for i, (index, filename) in enumerate(snapshot_files):
                if i >= 3:  # Delete snapshots beyond the 3rd
                    file_path = os.path.join(self.snapshot_dir, filename)
                    os.remove(file_path)
                    logger.debug(f"Removed old snapshot: {filename}")
                    
        except Exception as e:
            logger.error(f"Failed to cleanup old snapshots: {e}")
    
    def clear_all_data(self):
        """Clear all persistent data (used for testing)"""
        with self.lock:
            try:
                # Remove state file
                if os.path.exists(self.state_file):
                    os.remove(self.state_file)
                
                # Remove all snapshots
                for filename in os.listdir(self.snapshot_dir):
                    if filename.startswith(f"{self.node_id}_snapshot_"):
                        os.remove(os.path.join(self.snapshot_dir, filename))
                
                logger.info("Cleared all persistent data")
                
            except Exception as e:
                logger.error(f"Failed to clear data: {e}")