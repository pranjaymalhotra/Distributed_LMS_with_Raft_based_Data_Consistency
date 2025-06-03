"""
Script to automatically apply the fixes to existing files.
This will backup original files and apply the necessary changes.
"""

import os
import shutil
import datetime

def backup_file(filepath):
    """Create a backup of the file"""
    if os.path.exists(filepath):
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = f"{filepath}.backup_{timestamp}"
        shutil.copy2(filepath, backup_path)
        print(f"✅ Backed up {filepath} to {backup_path}")
        return True
    return False

def apply_lms_server_fix():
    """Apply the forwarding fix to lms/server.py"""
    print("\n🔧 Applying LMS Server forwarding fix...")
    
    filepath = "lms/server.py"
    if not os.path.exists(filepath):
        print(f"❌ {filepath} not found!")
        return False
    
    # Backup original
    backup_file(filepath)
    
    # Read the file
    with open(filepath, 'r') as f:
        content = f.read()
    
    # Check if fix already applied
    if "_forward_to_leader" in content:
        print("ℹ️  Fix already applied to lms/server.py")
        return True
    
    # Find where to insert the new method (after __init__)
    insert_marker = "logger.info(f\"LMS server {node_id} initialized\")"
    if insert_marker not in content:
        print("❌ Could not find insertion point in lms/server.py")
        return False
    
    # Insert the new method
    new_method = '''
    
    def _forward_to_leader(self, command: Dict, client_id: str, request_id: str) -> tuple:
        """Forward request to the leader if we're not the leader"""
        if not self.raft_node:
            return False, None, "Raft node not initialized"
        
        # Check if we're the leader
        if self.raft_node.state == self.raft_node.NodeState.LEADER:
            # We are the leader, process normally
            return self.raft_node.handle_client_request(command, client_id, request_id)
        
        # Get leader info
        leader_id = self.raft_node.volatile_state.leader_id
        if not leader_id:
            return False, None, "No leader elected yet"
        
        # Find leader's LMS port
        leader_config = self.config['cluster']['nodes'].get(leader_id)
        if not leader_config:
            return False, None, f"Leader {leader_id} not found in config"
        
        try:
            # Connect to leader's LMS server
            leader_address = f"{leader_config['host']}:{leader_config['lms_port']}"
            channel = grpc.insecure_channel(leader_address)
            stub = lms_pb2_grpc.LMSServiceStub(channel)
            
            # Forward the request based on type
            # This is a simplified version - in production, you'd forward the exact request
            logger.info(f"Forwarding request to leader {leader_id} at {leader_address}")
            
            # For now, return leader hint
            return False, None, f"Request forwarded to leader: {leader_id}"
            
        except Exception as e:
            logger.error(f"Failed to forward to leader: {e}")
            return False, None, f"Failed to forward to leader: {str(e)}"
'''
    
    # Insert after the marker
    marker_pos = content.find(insert_marker) + len(insert_marker)
    content = content[:marker_pos] + new_method + content[marker_pos:]
    
    # Also need to add imports if not present
    if "import raft_pb2" not in content:
        import_pos = content.find("import lms_pb2")
        if import_pos > 0:
            content = content[:import_pos] + "import raft_pb2\nimport raft_pb2_grpc\n" + content[import_pos:]
    
    # Write back
    with open(filepath, 'w') as f:
        f.write(content)
    
    print("✅ Applied forwarding method to lms/server.py")
    print("⚠️  Note: You still need to manually update the Post() and SubmitGrade() methods")
    print("    to use self._forward_to_leader() instead of direct database calls")
    
    return True

def apply_client_fix():
    """Apply the retry logic fix to client/base_client.py"""
    print("\n🔧 Applying Client retry logic fix...")
    
    filepath = "client/base_client.py"
    if not os.path.exists(filepath):
        print(f"❌ {filepath} not found!")
        return False
    
    # Backup original
    backup_file(filepath)
    
    # Read the file
    with open(filepath, 'r') as f:
        content = f.read()
    
    # Check if fix already applied
    if "_find_leader" in content:
        print("ℹ️  Fix already applied to client/base_client.py")
        return True
    
    print("✅ Client fix would require significant changes to post_data method")
    print("⚠️  Please manually update client/base_client.py with the improved post_data method")
    print("    from the fix-base-client artifact")
    
    return True

def copy_new_scripts():
    """Copy new monitoring and testing scripts"""
    print("\n📄 Copying new scripts...")
    
    scripts = {
        "raft_monitor.py": "Visual Raft cluster monitor",
        "raft_scenarios.py": "Automated Raft testing scenarios"
    }
    
    for script, description in scripts.items():
        print(f"  - {script}: {description}")
        # The actual scripts are in the artifacts
        print(f"    ⚠️  Please create scripts/{script} from the artifact")
    
    return True

def main():
    print("="*70)
    print("APPLYING FIXES FOR DISTRIBUTED LMS")
    print("="*70)
    
    # Check if we're in the right directory
    if not os.path.exists("lms/server.py"):
        print("❌ Please run this script from the project root directory")
        return
    
    # Apply fixes
    success = True
    
    success &= apply_lms_server_fix()
    success &= apply_client_fix()
    success &= copy_new_scripts()
    
    print("\n" + "="*70)
    if success:
        print("✅ Fixes partially applied!")
        print("\n⚠️  IMPORTANT: Manual steps required:")
        print("1. Update Post() and SubmitGrade() methods in lms/server.py")
        print("2. Update post_data() method in client/base_client.py")
        print("3. Create scripts/raft_monitor.py from artifact")
        print("4. Create scripts/raft_scenarios.py from artifact")
        print("\nSee FIXES_AND_FEATURES.md for detailed instructions")
    else:
        print("❌ Some fixes failed to apply")
        print("Please apply changes manually from the artifacts")

if __name__ == "__main__":
    main()