### Bonus CDI Data Product

#### 📌 Overview
This project calculates daily interest bonuses for user wallet balances based on the CDI (Certificado de Depósito Interbancário) rate. The pipeline is implemented using PySpark and is designed to run on Databricks.

#### 🧱 Project Structure

```
bonus-cdi/
├── databases/         # datalake layers
│   ├── bronze_layer  
│   ├── silver_layer  
│   └── gold_layer
├── scripts/
│   ├── bz_layer  # process files (CDC)
│   ├── sv_layer  # Intermediate historical
│   └── gd_layer  # Final bonus cdi
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
