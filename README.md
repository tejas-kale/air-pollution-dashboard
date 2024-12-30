# Air Pollution Dashboard

A comprehensive dashboard for visualizing and analyzing air pollution data across multiple cities.

## Features

- Historical air pollution data collection from OpenWeatherMap API
- BigQuery data storage
- Data transformation using dbt
- Interactive visualizations with Plotly and Streamlit
- Multiple pollutant analysis:
  - PM2.5 and PM10
  - NO2 and SO2
  - CO
  - O3 (including peak season analysis)
- WHO Air Quality Guidelines reference
- Data quality metrics
- Configurable cities and pollutant parameters
- CI/CD pipeline with Github Actions

## Installation

1. Create and activate virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # Unix/macOS
.venv\Scripts\activate     # Windows
```

2. Install the package:
```bash
pip install -e ".[dev]"
```

3. Set up environment variables:
```bash
OPENWEATHERMAP_API_KEY=your_api_key
GOOGLE_APPLICATION_CREDENTIALS=path/to/credentials.json
BIGQUERY_PROJECT_ID=your_project_id
```

4. Configure cities in two locations:
- `air_pollution_analytics/seeds/cities.csv`
- `config/cities.yml`

## Usage

1. Collect data:
```bash
python src/raw_data_collection/main.py
```

2. Run dbt models:
```bash
cd air_pollution_analytics
dbt run
```

3. Launch dashboard:
```bash
streamlit run src/web_app/app.py
```

## Architecture Overview

### High-Level Architecture

```mermaid
graph LR
    subgraph Data Collection
        DC["Data Collection 
        OpenWeatherMap API
        Google BigQuery"]
    end
    
    subgraph Data Processing
        DP["Data Processing
        dbt Core
        SQL"]
    end
    
    subgraph Visualization
        VA["Visualization & Analysis
        Streamlit
        Plotly"]
    end
    
    DC --> DP
    DP --> VA
    
    subgraph Configuration
        CF["Configuration
        YAML
        Environment Variables"] --> |Configure| DC
        CF --> |Configure| DP
        CF --> |Configure| VA
    end

    style DC fill:#f9f,stroke:#333
    style DP fill:#bbf,stroke:#333
    style VA fill:#bfb,stroke:#333
    style CF fill:#fdb,stroke:#333
```

### Detailed System Architecture

```mermaid
graph TB
    subgraph External Services
        OW[OpenWeatherMap API]
        GC[Google Cloud Platform]
    end

    subgraph Data Collection Layer
        C[Air Pollution Collector]
        OW -->|HTTP Requests| C
        C -->|Insert| BQ[(BigQuery)]
        GC ---|Authentication| BQ
    end

    subgraph Data Processing Layer
        BQ -->|Source| DBT[dbt Core]
        DBT -->|Transform| STG[(Staging Tables)]
        
        subgraph Data Models
            M1[Rolling Means]
            M2[Annual Means]
            M3[Peak Season Analysis]
            
            STG --> M1 & M2 & M3
        end
    end

    subgraph Application Layer
        subgraph Backend Services
            DM[Data Module] -->|Query| STG
            DM -->|Provide Data| PM[Plot Module]
            PM -->|Generate| VIZ[Visualizations]
        end
        
        subgraph Frontend
            VIZ -->|Display| UI[Streamlit UI]
            UI -->|User Input| DM
        end
    end

    subgraph Configuration Layer
        YML1[cities.yml]
        YML2[pollutants.yml]
        ENV[Environment Variables]
        
        YML1 & YML2 -->|Configure| C
        YML1 & YML2 -->|Configure| DM
        ENV -->|API Keys| C
        ENV -->|Credentials| BQ
    end

    style OW fill:#f9f,stroke:#333
    style GC fill:#bbf,stroke:#333
    style BQ fill:#bfb,stroke:#333
    style DBT fill:#fdb,stroke:#333
    style UI fill:#dfd,stroke:#333
```

### Component Architecture

```mermaid
graph TB
    subgraph Data Collection
        OW[OpenWeatherMap API] --> |HTTP Requests| C[Air Pollution Collector]
        C --> |Insert| BQ[(BigQuery Raw Data)]
    end

    subgraph Data Processing
        BQ --> |Source| DBT[dbt Models]
        DBT --> |Transform| STG[(Staging Tables)]
        
        subgraph Staging Models
            STG --> RM[Rolling Means]
            STG --> AM[Annual Means]
            STG --> O3[O3 Analysis]
            
            RM --> |24h Rolling Avg| RM1[PM2.5/PM10]
            RM --> |24h Rolling Avg| RM2[NO2/SO2]
            RM --> |24h Rolling Avg| RM3[CO]
            
            O3 --> |8h Rolling Max| O31[Daily Max]
            O3 --> |Peak Season| O32[Seasonal Analysis]
            
            AM --> |Annual Mean| AM1[PM2.5/PM10]
            AM --> |Annual Mean| AM2[NO2]
        end
    end

    subgraph Web Application
        subgraph Backend
            STG --> |Query| D[Data Module]
            D --> |Fetch| P[Plot Module]
            P --> |Visualize| PG[Pages Module]
        end
        
        subgraph Frontend
            PG --> |Render| T1[Overview Tab]
            PG --> |Render| T2[PM2.5 Tab]
            PG --> |Render| T3[PM10 Tab]
            PG --> |Render| T4[NO2 Tab]
            PG --> |Render| T5[SO2 Tab]
            PG --> |Render| T6[CO Tab]
            PG --> |Render| T7[O3 Tab]
        end
    end

    subgraph Configuration
        YML1[cities.yml] --> |Load| U[Utils Module]
        YML2[pollutants.yml] --> |Load| U
        U --> D
        U --> P
        U --> PG
    end

    style OW fill:#f9f,stroke:#333
    style BQ fill:#bbf,stroke:#333
    style STG fill:#bfb,stroke:#333
    style DBT fill:#fbb,stroke:#333
```

## Project Structure

```
air-pollution-dashboard/
├── .github/
│   └── workflows/
│       ├── daily-etl.yml          # Daily data collection pipeline
│       └── dbt-test.yml           # dbt testing pipeline
│
├── air_pollution_analytics/        # dbt project
│   ├── models/
│   │   └── staging/
│   │       ├── stg_annual_mean.sql
│   │       ├── stg_rolling_24h_mean.sql
│   │       ├── stg_o3_8h_rolling.sql
│   │       └── stg_o3_peak_season.sql
│   ├── seeds/
│   │   └── cities.csv             # Reference data for cities
│   ├── dbt_project.yml
│   └── profiles.yml
│
├── config/
│   └── cities.yml                 # Cities configuration for data collection
│
├── src/
│   ├── raw_data_collection/
│   │   ├── __init__.py
│   │   ├── air_pollution_collector.py
│   │   └── main.py
│   ├── utils/
│   │   ├── __init__.py
│   │   └── bq_utils.py
│   └── web_app/
│       ├── __init__.py
│       ├── app.py                 # Main Streamlit application
│       ├── data.py               # Data fetching functions
│       ├── plots.py              # Visualization functions
│       └── utils.py              # Utility functions
│
├── .env.example                   # Example environment variables
├── .gitignore
├── LICENSE
├── README.md
├── pyproject.toml                 # Project metadata and dependencies
└── requirements.txt               # Project dependencies
```

Each directory serves a specific purpose:
- `.github/workflows/`: CI/CD pipelines for automated data collection and testing
- `air_pollution_analytics/`: dbt project for data transformation
- `config/`: Configuration files for data collection
- `src/`: Source code organized by functionality
  - `raw_data_collection/`: Scripts for collecting data from OpenWeatherMap
  - `utils/`: Shared utility functions
  - `web_app/`: Streamlit dashboard application

## Data Flow

1. **Data Collection**:
   - Fetches air pollution data from OpenWeatherMap API
   - Stores raw data in BigQuery

2. **Data Processing**:
   - dbt models transform raw data into analysis-ready tables
   - Calculates rolling means, annual averages, and specialized metrics

3. **Visualization**:
   - Streamlit web app fetches processed data
   - Creates interactive charts using Plotly
   - Displays data quality metrics

## License

MIT License
