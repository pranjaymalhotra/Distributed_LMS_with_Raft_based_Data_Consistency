# Distributed Learning Management System with Raft Consensus

A production-grade distributed Learning Management System (LMS) that features:
- **Full Raft Consensus Protocol** for data consistency across nodes
- **LLM-based Adaptive Tutoring** using Ollama (deepseek-r1:1.5b)
- **Fault Tolerance** with automatic leader election and log replication
- **gRPC-based Communication** for efficient inter-node communication
- **Role-based Access Control** for students and instructors

## 🏗️ Architecture Overview

### System Components

1. **Raft Consensus Layer**
   - Leader election with randomized timeouts
   - Log replication across all nodes
   - Snapshot support for log compaction
   - Dynamic membership changes
   - Network partition handling

2. **LMS Servers (5 nodes)**
   - Each node runs both Raft consensus and LMS service
   - All critical data replicated via Raft
   - Automatic failover and recovery
   - Consistent data across all nodes

3. **Tutoring Server**
   - Integrates with Ollama for LLM capabilities
   - Adaptive difficulty based on student performance
   - Context-aware responses using course materials
   - Processes PDF and TXT course materials

4. **Client Applications**
   - Student client for assignments, queries, and progress tracking
   - Instructor client for course management and grading
   - Automatic server discovery and failover

## 📋 Prerequisites

- Python 3.8 or higher
- Ollama installed and running
- deepseek-r1:1.5b model pulled in Ollama
- 8GB+ RAM recommended
- Linux/macOS/Windows with WSL2

## 🚀 Quick Start

### 1. Clone the Repository
```bash
git clone https://github.com/pranjaymalhotra/Distributed_LMS_with_Raft_based_Data_Consistency.git
cd Distributed_LMS_with_Raft_based_Data_Consistency
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Install Ollama and Pull Model
```bash
# Install Ollama (if not already installed)
curl -fsSL https://ollama.ai/install.sh | sh

# Pull the required model
ollama pull deepseek-r1:1.5b

# Verify Ollama is running
ollama list
```

### 4. Generate Protocol Buffers
```bash
chmod +x scripts/generate_protos.sh
./scripts/generate_protos.sh
```

### 5. Start the Cluster
```bash
python scripts/start_cluster.py
```

This will start:
- 5 LMS nodes with Raft consensus
- 1 Tutoring server with LLM integration

### 6. Run Client Applications

In separate terminals:

**For Students:**
```bash
python -m client.student_client.py
# Login with: username: student1, password: pass123
```

**For Instructors:**
```bash
python -m client.instructor_client
# Login with: username: instructor1, password: pass123
```

## 🔧 Configuration

Edit `config.json` to customize:
- Node addresses and ports
- Raft timing parameters
- Storage directories
- Authentication settings
- Ollama model selection

### Default Users
- **Instructor**: instructor1 / pass123
- **Students**: student1, student2, student3 / pass123

## 📚 Features

### For Students
- View and submit assignments
- Track grades and progress
- Post queries (to AI tutor or instructor)
- Download course materials
- Adaptive tutoring based on performance level

### For Instructors
- Create assignments with attachments
- Grade submissions with feedback
- Upload course materials (PDF/TXT)
- Answer student queries
- View class statistics and individual progress

### Raft Consensus Features
- **Leader Election**: Automatic leader election with split-vote prevention
- **Log Replication**: All changes replicated to majority before commit
- **Fault Tolerance**: System remains available with (n/2)+1 nodes
- **Consistency**: Strong consistency guarantees for all operations
- **Log Compaction**: Automatic snapshots to prevent unbounded log growth

## 🏛️ System Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Student       │     │   Instructor    │     │   Admin Tools   │
│   Client        │     │   Client        │     │                 │
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                         │
         └───────────────────────┴─────────────────────────┘
                                 │
                    ┌────────────┴────────────┐
                    │   Load Balancer /       │
                    │   Service Discovery     │
                    └────────────┬────────────┘
                                 │
     ┌───────────────────────────┼───────────────────────────┐
     │                           │                           │
┌────┴──────┐  ┌────────────┐  ┌┴───────────┐  ┌───────────┴─┐  ┌─────────────┐
│ LMS Node1 │  │ LMS Node2  │  │ LMS Node3  │  │ LMS Node4   │  │ LMS Node5   │
│  (Raft)   │◄─┤   (Raft)   │◄─┤   (Raft)   │◄─┤   (Raft)    │◄─┤   (Raft)    │
└───────────┘  └────────────┘  └────────────┘  └─────────────┘  └─────────────┘
      │               │               │               │                │
      └───────────────┴───────────────┴───────────────┴────────────────┘
                                      │
                            ┌─────────┴─────────┐
                            │  Tutoring Server  │
                            │   (Ollama LLM)    │
                            └───────────────────┘
```

## 🔍 Raft Implementation Details

### State Machine Operations
- CREATE_ASSIGNMENT
- SUBMIT_ASSIGNMENT
- GRADE_ASSIGNMENT
- POST_QUERY
- ANSWER_QUERY
- UPLOAD_MATERIAL
- UPDATE_STUDENT_PROGRESS

### Persistent State
- Current term
- Voted for
- Log entries
- Snapshots

### Volatile State
- Commit index
- Last applied
- Leader state (nextIndex, matchIndex)

### RPC Messages
- RequestVote
- AppendEntries
- InstallSnapshot
- ForwardRequest (client request forwarding)

## 🧪 Testing

### Unit Tests
```bash
pytest tests/test_raft.py -v
pytest tests/test_lms.py -v
```

### Integration Tests
```bash
pytest tests/test_integration.py -v
```

### Raft Consensus Testing
```bash
python scripts/test_raft.py
```

This will test:
- Leader election scenarios
- Network partitions
- Log replication consistency
- Node failures and recovery

## 📊 Monitoring

The system logs important metrics:
- Elections started/won
- Heartbeats sent
- Entries appended/committed
- Snapshots created
- Request latencies

Check logs in the `logs/` directory.

## 🚨 Troubleshooting

### Common Issues

1. **Ollama not running**
   ```bash
   # Start Ollama service
   ollama serve
   ```

2. **Port already in use**
   - Check `config.json` and adjust port numbers
   - Use `scripts/stop_cluster.py` to clean up

3. **Model not found**
   ```bash
   ollama pull deepseek-r1:1.5b
   ```

4. **Proto generation fails**
   ```bash
   pip install grpcio-tools --upgrade
   ```

## 🔐 Security Considerations

- Token-based authentication (24-hour expiry)
- Role-based access control
- All nodes communicate over insecure channels (use TLS in production)
- Passwords stored as SHA-256 hashes

## 🚧 Production Deployment

For production use:
1. Enable TLS for all gRPC connections
2. Use external configuration management
3. Implement proper backup strategies
4. Set up monitoring and alerting
5. Use container orchestration (Kubernetes)
6. Configure firewall rules for Raft/LMS ports

## 📈 Performance

- Supports hundreds of concurrent users
- Sub-second response times for most operations
- Raft can handle ~1000 operations/second
- LLM responses depend on Ollama performance

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- Raft consensus algorithm by Diego Ongaro and John Ousterhout
- Ollama for local LLM deployment
- gRPC for efficient RPC communication