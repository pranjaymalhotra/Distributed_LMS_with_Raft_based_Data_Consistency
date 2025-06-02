# Quick Start Guide - Distributed LMS

## 🚀 5-Minute Setup

### Prerequisites Check
```bash
# Check Python version (need 3.8+)
python --version

# Check if Ollama is installed
ollama --version

# If Ollama not installed:
curl -fsSL https://ollama.ai/install.sh | sh
```

### Step 1: Install Dependencies
```bash
# Clone the repository (or extract files)
cd distributed-lms

# Install Python dependencies
pip install -r requirements.txt

# Download NLTK data (for tutoring)
python -c "import nltk; nltk.download('punkt'); nltk.download('stopwords')"
```

### Step 2: Setup Ollama
```bash
# Start Ollama service (in a separate terminal)
ollama serve

# Pull the required model (in another terminal)
ollama pull deepseek-r1:1.5b
```

### Step 3: Generate Protocol Buffers
```bash
# Make script executable
chmod +x scripts/generate_protos.sh

# Generate Python files from proto definitions
./scripts/generate_protos.sh
```

### Step 4: Start the Cluster
```bash
# Start all nodes (5 LMS nodes + 1 tutoring server)
python scripts/start_cluster.py
```

You should see:
```
✅ CLUSTER STARTED SUCCESSFULLY!
Press Ctrl+C to stop the cluster
```

### Step 5: Run Clients

**For Students** (new terminal):
```bash
python -m client.student_client.py


# Login with:
# Username: student1
# Password: pass123
```

**For Instructors** (new terminal):
```bash
python -m client.instructor_client

# Login with:
# Username: instructor1
# Password: pass123
```

## 🎯 Quick Test Workflow

### As Instructor:
1. Create Assignment (Menu option 1)
2. Upload Course Material (Menu option 5)

### As Student:
1. View Assignments (Menu option 1)
2. Submit Assignment (Menu option 2)
3. Post Query to AI Tutor (Menu option 7)

### Back as Instructor:
4. Grade Assignment (Menu option 4)
5. View Class Statistics (Menu option 11)

## 🛠️ Troubleshooting

### "Ollama not found"
```bash
# Install Ollama
curl -fsSL https://ollama.ai/install.sh | sh

# Start Ollama
ollama serve
```

### "Model not found"
```bash
# Pull the model
ollama pull deepseek-r1:1.5b
```

### "Port already in use"
```bash
# Stop any running clusters
python scripts/stop_cluster.py

# Or kill specific port (example for port 6001)
lsof -ti:6001 | xargs kill -9
```

### "Import error for proto files"
```bash
# Regenerate proto files
python -m grpc_tools.protoc -I protos --python_out=. --grpc_python_out=. protos/*.proto
```

## 📊 Monitoring

### Check Cluster Status
While cluster is running, you'll see real-time status updates.

### View Logs
```bash
# All logs
tail -f logs/lms.log

# Specific node logs
grep "node1" logs/lms.log
```

### Test Raft Consensus
```bash
# Run Raft-specific tests
python scripts/test_raft.py
```

## 🏃 Running Individual Nodes

### Start a Single LMS Node
```bash
python lms/main.py node1 --config config.json
```

### Start Only Tutoring Server
```bash
python tutoring/main.py --config config.json
```

## 🔄 Clean Restart

### Full Reset
```bash
# Stop cluster
python scripts/stop_cluster.py

# Remove all data
rm -rf data/ logs/

# Start fresh
python scripts/start_cluster.py
```

## 📝 Default Users

| Type | Username | Password | Name |
|------|----------|----------|------|
| Instructor | instructor1 | pass123 | Dr. Smith |
| Student | student1 | pass123 | Alice Johnson |
| Student | student2 | pass123 | Bob Wilson |
| Student | student3 | pass123 | Charlie Davis |

## 🎓 Key Features to Try

1. **Fault Tolerance**: Stop a node while running - system continues working
2. **Consistency**: Create data on one node, access from another
3. **AI Tutoring**: Ask questions as a student with "AI Tutor" option
4. **Adaptive Learning**: AI adjusts based on student performance level
5. **Leader Election**: Watch automatic leader election in logs

## 📚 Next Steps

- Read the full [README.md](README.md) for detailed documentation
- Explore the [Raft implementation](raft/node.py) for consensus details
- Customize the [configuration](config.json) for your needs
- Run the [test suite](tests/) to verify everything works

## 💡 Tips

- Keep Ollama running in the background for AI features
- Use multiple terminals for different clients to simulate real usage
- Check logs for understanding Raft consensus in action
- Try network partition tests by stopping nodes