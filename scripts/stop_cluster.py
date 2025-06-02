"""
Script to stop the distributed LMS cluster.
Gracefully shuts down all nodes and servers.
"""

import os
import sys
import psutil
import signal
import time

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def find_lms_processes():
    """Find all LMS-related processes"""
    lms_processes = []
    
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            cmdline = proc.info['cmdline']
            if cmdline and any('raft.node' in str(cmd) or 
                             'lms.server' in str(cmd) or 
                             'tutoring.server' in str(cmd) for cmd in cmdline):
                lms_processes.append(proc)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    
    return lms_processes


def stop_process(proc):
    """Stop a process gracefully, then forcefully if needed"""
    try:
        print(f"  Stopping PID {proc.pid}...")
        proc.terminate()
        
        # Wait for graceful shutdown
        try:
            proc.wait(timeout=5)
            print(f"  ✅ PID {proc.pid} stopped gracefully")
        except psutil.TimeoutExpired:
            print(f"  ⚠️  PID {proc.pid} didn't stop, forcing...")
            proc.kill()
            proc.wait(timeout=2)
            print(f"  ✅ PID {proc.pid} force stopped")
            
    except psutil.NoSuchProcess:
        print(f"  Process {proc.pid} already stopped")
    except Exception as e:
        print(f"  ❌ Error stopping PID {proc.pid}: {e}")


def main():
    """Main function to stop the cluster"""
    print("\n" + "="*60)
    print("STOPPING DISTRIBUTED LMS CLUSTER")
    print("="*60)
    
    # Find LMS processes
    print("\nSearching for LMS processes...")
    processes = find_lms_processes()
    
    if not processes:
        print("No LMS processes found running.")
        return
    
    print(f"\nFound {len(processes)} LMS processes:")
    for proc in processes:
        try:
            cmdline = ' '.join(proc.cmdline())
            if 'tutoring.server' in cmdline:
                proc_type = "Tutoring Server"
            elif 'raft.node' in cmdline:
                proc_type = "LMS Node"
            else:
                proc_type = "LMS Process"
            
            print(f"  - PID {proc.pid}: {proc_type}")
        except:
            print(f"  - PID {proc.pid}: Unknown")
    
    # Confirm shutdown
    confirm = input("\nStop all processes? (y/n): ")
    if confirm.lower() != 'y':
        print("Cancelled.")
        return
    
    # Stop all processes
    print("\nStopping processes...")
    for proc in processes:
        stop_process(proc)
    
    print("\n✅ All LMS processes stopped!")
    
    # Optional: Clean up data files
    cleanup = input("\nClean up data files? (y/n): ")
    if cleanup.lower() == 'y':
        print("\nCleaning up data files...")
        
        data_dirs = [
            'data/raft_logs',
            'data/raft_snapshots',
            'data/database',
            'logs'
        ]
        
        for data_dir in data_dirs:
            if os.path.exists(data_dir):
                print(f"  Cleaning {data_dir}...")
                # Here you would implement actual cleanup
                # For safety, we're just listing what would be cleaned
                
        print("  ⚠️  Data cleanup not implemented (for safety)")
        print("  Please manually clean data directories if needed")


if __name__ == "__main__":
    main()