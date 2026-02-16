# CloudLakeX

<div align="center">

![CloudLakeX Logo](https://img.shields.io/badge/CloudLakeX-Open%20Data%20Lakehouse-blue?style=for-the-badge&logo=googlecloud&logoColor=white)

**A comprehensive open-source data lakehouse solution built on Google Cloud Platform**

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://www.python.org/)
[![Terraform](https://img.shields.io/badge/Terraform-Ready-green.svg)](https://www.terraform.io/)

</div>

---

## 🌟 Overview

**CloudLakeX** is an enterprise-grade data lakehouse platform that combines the flexibility of open-source data lake architectures with the power of Google Cloud Platform's native services. Built on Apache Iceberg, Apache Spark, and Apache Kafka, CloudLakeX enables organizations to build scalable, multi-platform data architectures while maintaining data portability and avoiding vendor lock-in.

### Key Features

- 🏗️ **Open Standards**: Built on Apache Iceberg for open table formats
- ☁️ **GCP Native Integration**: Leverages BigQuery, Dataproc, Cloud Storage, and Vertex AI
- 🔄 **Streaming & Batch**: Supports both real-time streaming and batch processing
- 📊 **Unified Analytics**: Access data from Spark, BigQuery, and other engines seamlessly
- 🤖 **AI/ML Ready**: Integrated with Vertex AI for advanced analytics and forecasting
- 🔐 **Enterprise Security**: Built-in governance with Dataplex and IAM controls
- 🚀 **Infrastructure as Code**: Complete Terraform deployment automation

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    CloudLakeX Architecture                  │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐   │
│  │   Apache     │    │   Apache     │    │   Google     │   │
│  │   Kafka      │───▶│   Spark      │───▶│   BigQuery  │   │
│  │  (Streaming) │    │ (Processing) │    │  (Analytics) │   │
│  └──────────────┘    └──────────────┘    └──────────────┘   │ 
│         │                    │                    │         │
│         └────────────────────┼────────────────────┘         │
│                              │                              │
│                    ┌─────────▼─────────┐                    │
│                    │  Apache Iceberg   │                    │
│                    │  (Table Format)   │                    │
│                    └─────────┬─────────┘                    │
│                              │                              │
│                    ┌─────────▼─────────┐                    │
│                    │  Google Cloud     │                    │
│                    │  Storage (GCS)    │                    │
│                    └───────────────────┘                    │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐   │
│  │         Vertex AI (ML & Forecasting)                 │   │
│  │         Dataplex (Governance)                        │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## 📦 Components

### 1. **Data Generation & Ingestion**
- Synthetic data generation using Faker
- Real-world MTA ridership data integration
- Kafka Connect for streaming data ingestion
- Batch data loading pipelines

### 2. **Data Processing**
- Apache Spark for distributed processing
- BigQuery for SQL-based analytics
- Iceberg table management
- Data transformation workflows

### 3. **Analytics & ML**
- Time-series forecasting with BigQuery ML
- Vertex AI integration for advanced analytics
- Interactive dashboards and visualizations
- Real-time monitoring and alerting

### 4. **Infrastructure**
- Terraform modules for complete GCP setup
- Cloud Run for web applications
- Dataproc for Spark clusters
- Managed Kafka infrastructure

---

## 🚀 Quick Start

### Prerequisites

- Google Cloud Platform account with billing enabled
- Terraform >= 1.0
- Python >= 3.11
- Google Cloud SDK (`gcloud`) installed and configured
- Git

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/theyashdhiman04/CloudLakeX.git
   cd CloudLakeX
   ```

2. **Set up Terraform variables**
   ```bash
   cd terraform
   cp terraform.tfvars.example terraform.tfvars
   # Edit terraform.tfvars with your GCP project details
   ```

3. **Deploy infrastructure**
   ```bash
   terraform init
   terraform plan
   terraform apply
   ```

4. **Set up Python environment**
   ```bash
   # For assets/notebooks
   cd assets
   uv sync  # or pip install -e .

   # For webapp
   cd webapp
   uv sync  # or pip install -e .
   ```

5. **Run notebooks**
   ```bash
   # Start with Part 0: Data Generation
   jupyter notebook assets/notebooks/lakehouse_part0_data_generation.ipynb
   ```

---

## 📚 Documentation

### Notebooks

The project includes comprehensive Jupyter notebooks that guide you through the entire data lakehouse setup:

1. **Part 0: Data Generation** (`lakehouse_part0_data_generation.ipynb`)
   - Generate synthetic datasets
   - Load real-world MTA ridership data
   - Prepare data for ingestion

2. **Part 1: Load Data** (`lakehouse_part1_load_data.ipynb`)
   - Create BigQuery Iceberg tables
   - Load data into GCS
   - Set up table schemas

3. **Part 2: Spark Processing** (`lakehouse_part2_spark_processing.ipynb`)
   - Process data with Apache Spark
   - Simulate bus rides and events
   - Unified data access patterns

4. **Part 3: Time-Series Forecasting** (`lakehouse_part3_time_series_forecasting.ipynb`)
   - Build forecasting models with BigQuery ML
   - Integrate with Vertex AI
   - Generate predictions and insights

### Web Application

The CloudLakeX dashboard provides real-time monitoring and visualization:

```bash
cd webapp
python -m buses_dashboard.main
```

Access the dashboard at `http://localhost:5000`

---

## 🛠️ Technology Stack

| Component | Technology |
|-----------|-----------|
| **Table Format** | Apache Iceberg |
| **Processing** | Apache Spark, BigQuery |
| **Streaming** | Apache Kafka, Kafka Connect |
| **Storage** | Google Cloud Storage |
| **Analytics** | BigQuery, BigQuery ML |
| **ML/AI** | Vertex AI |
| **Governance** | Dataplex |
| **Infrastructure** | Terraform |
| **Web Framework** | Flask |
| **Frontend** | Tailwind CSS |

---

## 📊 Use Cases

- **Transportation Analytics**: Real-time bus ridership tracking and forecasting
- **IoT Data Processing**: High-volume sensor data ingestion and analysis
- **Financial Analytics**: Transaction processing and fraud detection
- **E-commerce**: Customer behavior analysis and recommendation systems
- **Healthcare**: Patient data analytics and predictive modeling

---

## 🔧 Configuration

### Environment Variables

```bash
export PROJECT_ID="your-gcp-project-id"
export REGION="us-central1"
export BQ_DATASET="ridership_lakehouse"
export BUCKET_NAME="${PROJECT_ID}-ridership-lakehouse"
```

### Terraform Variables

Key variables in `terraform/terraform.tfvars`:

```hcl
project_id = "your-gcp-project-id"
region     = "us-central1"
zone       = "us-central1-a"
```

---

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 📝 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

---

## 🙏 Acknowledgments

- Google Cloud Platform for infrastructure services
- Apache Software Foundation for open-source tools (Iceberg, Spark, Kafka)
- MTA for providing open ridership data
- The open-source community for inspiration and support

---

## 📧 Contact

**CloudLakeX** - [@theyashdhiman04](https://github.com/theyashdhiman04)

Project Link: [https://github.com/theyashdhiman04/CloudLakeX](https://github.com/theyashdhiman04/CloudLakeX)

---

## ⭐ Show Your Support

If you find CloudLakeX useful, please consider giving it a star ⭐ on GitHub!

---

<div align="center">

**Built with ❤️ using open-source technologies**

[Report Bug](https://github.com/theyashdhiman04/CloudLakeX/issues) · [Request Feature](https://github.com/theyashdhiman04/CloudLakeX/issues) · [Documentation](https://github.com/theyashdhiman04/CloudLakeX/wiki)

</div>


