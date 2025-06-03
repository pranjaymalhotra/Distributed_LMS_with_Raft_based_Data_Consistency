"""
Raft log implementation with support for:
- Log entries with term and index
- Log truncation for snapshots
- Efficient log matching
- Persistence to disk
"""

import json
import os
import time
import threading
from typing import List, Optional, Dict, Tuple
from dataclasses import dataclass, asdict
from common.logger import get_logger

logger = get_logger(__name__)


@dataclass
class LogEntry:
    """A single log entry in the Raft log"""
    index: int
    term: int
    command: Dict  # The actual command to be applied to the state machine
    client_id: str = ""
    request_id: str = ""
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization"""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'LogEntry':
        """Create from dictionary"""
        return cls(**data)


class RaftLog:
    """
    Raft log management with persistence.
    Handles log operations, snapshots, and disk persistence.
    """
    
    def __init__(self, node_id: str, log_dir: str):
        self.node_id = node_id
        self.log_dir = log_dir
        self.log_file = os.path.join(log_dir, f"{node_id}_log.json")
        self.entries: List[LogEntry] = []
        self.snapshot_index = 0  # Index of last entry in snapshot
        self.snapshot_term = 0   # Term of last entry in snapshot
        self.lock = threading.RLock()
        
        # Create log directory if it doesn't exist
        os.makedirs(log_dir, exist_ok=True)
        
        # Load existing log from disk
        self._load_from_disk()
    
    def append_entry(self, entry: LogEntry) -> bool:
        """Append a new entry to the log"""
        with self.lock:
            # Ensure entry index is correct
            expected_index = self.get_last_index() + 1
            if entry.index != expected_index:
                logger.error(f"Invalid entry index: expected {expected_index}, got {entry.index}")
                return False
            
            self.entries.append(entry)
            self._persist_to_disk()
            return True
    
    def append_entries(self, prev_index: int, prev_term: int, entries: List[LogEntry]) -> bool:
        """
        Append multiple entries to the log.
        Returns True if successful, False if log doesn't contain prev_index/prev_term.
        """
        with self.lock:
            # Check if we have the previous entry
            if prev_index > 0:
                if not self.has_entry(prev_index, prev_term):
                    return False
                
                # Remove any conflicting entries
                if prev_index < self.get_last_index():
                    # Find the point of divergence
                    local_prev_term = self.get_term_at_index(prev_index)
                    if local_prev_term != prev_term:
                        # Delete all entries from prev_index onwards
                        self._truncate_from_index(prev_index)
                    else:
                        # Check for conflicts in new entries
                        for new_entry in entries:
                            if new_entry.index <= self.get_last_index():
                                existing_term = self.get_term_at_index(new_entry.index)
                                if existing_term != new_entry.term:
                                    # Conflict found, truncate from this point
                                    self._truncate_from_index(new_entry.index)
                                    break
            
            # Append new entries
            for entry in entries:
                if entry.index > self.get_last_index():
                    self.entries.append(entry)
            
            if entries:  # Only persist if we actually added entries
                self._persist_to_disk()
            
            return True
    
    def get_entry(self, index: int) -> Optional[LogEntry]:
        """Get entry at specified index"""
        with self.lock:
            if index <= self.snapshot_index:
                return None  # Entry is in snapshot
            
            array_index = index - self.snapshot_index - 1
            if 0 <= array_index < len(self.entries):
                return self.entries[array_index]
            return None
    
    def get_entries_from(self, start_index: int, max_entries: Optional[int] = None) -> List[LogEntry]:
        """Get entries starting from start_index"""
        with self.lock:
            if start_index <= self.snapshot_index:
                start_index = self.snapshot_index + 1
            
            array_start = start_index - self.snapshot_index - 1
            if array_start < 0 or array_start >= len(self.entries):
                return []
            
            if max_entries is None:
                return self.entries[array_start:]
            else:
                return self.entries[array_start:array_start + max_entries]
    
    def has_entry(self, index: int, term: int) -> bool:
        """Check if we have an entry at index with the given term"""
        with self.lock:
            entry = self.get_entry(index)
            return entry is not None and entry.term == term
    
    def get_term_at_index(self, index: int) -> int:
        """Get the term of entry at specified index"""
        with self.lock:
            if index == self.snapshot_index:
                return self.snapshot_term
            
            entry = self.get_entry(index)
            return entry.term if entry else 0
    
    def get_last_index(self) -> int:
        """Get the index of the last entry in the log"""
        with self.lock:
            if self.entries:
                return self.entries[-1].index
            return self.snapshot_index
    
    def get_last_term(self) -> int:
        """Get the term of the last entry in the log"""
        with self.lock:
            if self.entries:
                return self.entries[-1].term
            return self.snapshot_term
    
    def find_conflict_index(self, leader_prev_index: int, leader_prev_term: int) -> Tuple[int, int]:
        """
        Find the conflict index and term for faster log reconciliation.
        Returns (conflict_index, conflict_term).
        """
        with self.lock:
            # If we don't have the entry at prev_index
            if leader_prev_index > self.get_last_index():
                return self.get_last_index() + 1, 0
            
            # If terms don't match at prev_index
            our_term = self.get_term_at_index(leader_prev_index)
            if our_term != leader_prev_term:
                # Find the first index of our_term
                conflict_term = our_term
                conflict_index = leader_prev_index
                
                # Walk backwards to find the start of this term
                while conflict_index > self.snapshot_index + 1:
                    prev_term = self.get_term_at_index(conflict_index - 1)
                    if prev_term != conflict_term:
                        break
                    conflict_index -= 1
                
                return conflict_index, conflict_term
            
            # No conflict
            return 0, 0
    
    def compact_log(self, snapshot_index: int, snapshot_term: int):
        """
        Compact the log by discarding entries up to snapshot_index.
        Used after creating a snapshot.
        """
        with self.lock:
            if snapshot_index <= self.snapshot_index:
                return  # Already compacted beyond this point
            
            # Find entries to keep (those after snapshot_index)
            new_entries = []
            for entry in self.entries:
                if entry.index > snapshot_index:
                    new_entries.append(entry)
            
            self.entries = new_entries
            self.snapshot_index = snapshot_index
            self.snapshot_term = snapshot_term
            
            self._persist_to_disk()
            logger.info(f"Compacted log up to index {snapshot_index}")
    
    def _truncate_from_index(self, index: int):
        """Remove all entries from index onwards"""
        array_index = index - self.snapshot_index - 1
        if array_index >= 0:
            self.entries = self.entries[:array_index]
    
    def _persist_to_disk(self):
        """Persist the log to disk"""
        try:
            data = {
                'snapshot_index': self.snapshot_index,
                'snapshot_term': self.snapshot_term,
                'entries': [entry.to_dict() for entry in self.entries]
            }
            
            # Write to temporary file first
            temp_file = self.log_file + '.tmp'
            with open(temp_file, 'w') as f:
                json.dump(data, f, indent=2)
            
            # Atomically rename
            os.replace(temp_file, self.log_file)
            
        except Exception as e:
            logger.error(f"Failed to persist log: {e}")
    
    def _load_from_disk(self):
        """Load the log from disk"""
        try:
            if os.path.exists(self.log_file):
                with open(self.log_file, 'r') as f:
                    data = json.load(f)
                
                self.snapshot_index = data.get('snapshot_index', 0)
                self.snapshot_term = data.get('snapshot_term', 0)
                self.entries = [LogEntry.from_dict(e) for e in data.get('entries', [])]
                
                logger.info(f"Loaded {len(self.entries)} log entries from disk")
        except Exception as e:
            logger.error(f"Failed to load log from disk: {e}")
            # Start with empty log on error
            self.entries = []
            self.snapshot_index = 0
            self.snapshot_term = 0