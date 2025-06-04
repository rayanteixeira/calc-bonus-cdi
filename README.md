### Bonus CDI Data Product

#### 📌 Overview
This project calculates daily interest bonuses for user wallet balances based on the CDI (Certificado de Depósito Interbancário) rate. The pipeline is implemented using PySpark and is designed to run on Databricks.

#### 🧱 Project Structure

```
bonus-cdi/
├── data/         # datalake layers
│   ├── bronze_layer  
│   └── raw_layer
│           ├── transactions20250529.json
│           ├── transactions20250530.json
│           ├── transactions20250531.json
│           ├── transactions20250601.json
│           ├── transactions20250602.json
│           └── cdi20250603.json
├── databases/ 
│   └── wallet 
├── scripts/
│   ├── bz_layer  # process files (CDC)
│   └── sv_layer  # Intermediate historical
├── docs/
│   └── design.md      # Technical documentation
├── docker-compose.yml # Configuration file by Docker
├── Dockerfile.md      # Instructions to create a Docker image
├── main_flow.py       # Main script
├── README.md          # Project instructions
└── requirements.txt   # Dependencies
```

#### ⚙️ Installation

1. Requirements

Docker:   
```
Docker installed: https://www.docker.com/products/docker-desktop 
Docker Compose: comes pre-installed with Docker Desktop on Windows/macOS. (Optional)
```

Git:
```
sudo apt update
sudo apt install git
```

2. Clone the repository:
```
git clone https://github.com/rayanteixeira/calc-bonus-cdi.git
```

3. Navigate to the code root folder
```
cd /Users/user/Documents/calc-bonus-cdi
```

4. Start the Docker container
```
docker-compose up -build
```

5. (Optional) Install SQLite Viewer in Visual Studio Code to visualize the data stored in the database.
```
Link: 
https://marketplace.visualstudio.com/items?itemName=qwtel.sqlite-viewer
```