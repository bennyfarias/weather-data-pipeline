# ⛅ End-to-End Weather Data Pipeline (ETL)

## 📖 Project Overview
This project demonstrates a complete Batch ETL (Extract, Transform, Load) pipeline. It extracts real-time weather data from a public API, stores the raw payload in a Data Lake (Simulating AWS S3), transforms the data using Pandas, and loads it into a PostgreSQL Data Warehouse for analytical purposes. 

This project showcases fundamental Data Engineering skills, including API integration, Data Lake architecture, containerization with Docker, and unit testing.

## 🏗️ Architecture Diagram

![Architecture Diagram](arquitetura.png)

1. **Extract:** Python script connects to the Open-Meteo API to fetch real-time weather JSON data.
2. **Data Lake (Raw Zone):** The raw JSON payload is saved to MinIO (an S3-compatible object storage) to preserve historical untampered data.
3. **Transform:** Pandas is used to parse the JSON, select relevant metrics (temperature, wind speed, etc.), and add auditing columns (extraction timestamp).
4. **Load (Refined Zone):** The cleaned data is loaded into a PostgreSQL database using SQLAlchemy.

## 🛠️ Technologies Used
* **Programming Language:** Python 3.12 (Pandas, Requests, Boto3, SQLAlchemy)
* **Infrastructure as Code (IaC):** Docker & Docker Compose
* **Data Lake:** MinIO
* **Data Warehouse:** PostgreSQL
* **Testing:** Pytest

## 🚀 How to Run Locally

### Prerequisites
* Docker Desktop installed and running.
* Python 3.9+ installed.

### Step-by-Step Instructions

1. **Clone the repository:**
   ```bash
   git clone [https://github.com/YOUR_USERNAME/YOUR_REPO_NAME.git](https://github.com/YOUR_USERNAME/YOUR_REPO_NAME.git)
   cd YOUR_REPO_NAME