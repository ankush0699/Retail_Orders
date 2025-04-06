# Retail Orders ETL Pipeline (MySQL + Kaggle + SQLAlchemy)

![Python](https://img.shields.io/badge/Python-3.7%2B-blue)
![MySQL](https://img.shields.io/badge/MySQL-Compatible-brightgreen)
![License](https://img.shields.io/badge/License-MIT-blue.svg)

This project automates the end-to-end ETL (Extract, Transform, Load) process for retail order data. It fetches data from Kaggle, processes and cleans it, and then loads it into a MySQL database. This pipeline is designed with scalability, modularity, and traceability in mind, making it suitable for both development and production environments.

---

## Key Features

- **Automated Dataset Retrieval**: Downloads the `ankitbansal06/retail-orders` dataset from Kaggle using the Kaggle API.
- **Data Preprocessing**: Cleans column names, handles missing values, performs type conversions, and calculates profit.
- **Database Integration**: Loads the cleaned dataset into a MySQL table named `df_orders`. Automatically creates the database if it doesn't exist.
- **SQLAlchemy Support**: Optional integration with SQLAlchemy for future extensibility.
- **Data Backup**: Stores a dated CSV backup for historical tracking and safety.
- **Robust Logging**: Implements rotating logs to monitor the ETL process and capture runtime events and errors.

---

## Getting Started

### 1. Clone the repository
```bash
git clone https://github.com/ankush0699/your-repo-name.git
cd your-repo-name
```

### 2. Create and activate a virtual environment
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

---

## Prerequisites

Ensure you have the following installed and configured:

- Python 3.7+
- MySQL Server
- MySQL client tools (for direct SQL execution via terminal or MySQL shell)
- [Kaggle API credentials](https://www.kaggle.com/account)

---

## Environment Configuration

Create a `.env` file in the root directory with the following variables:

```
DB_USER=your_username
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=3306
DB_NAME=your_database
```

### Kaggle Configuration

1. Download `kaggle.json` from your Kaggle account settings.
2. Place the file in your user directory under `.kaggle/`
   - Windows: `C:\Users\<YourUsername>\.kaggle\kaggle.json`
   - Linux/Mac: `/home/<YourUsername>/.kaggle/kaggle.json`

Alternatively, set the following environment variables:

```
KAGGLE_USERNAME=your_kaggle_username
KAGGLE_KEY=your_kaggle_api_key
```

---

## Project Structure

```
.
├── .env                      # Environment variables
├── .gitignore                # Git ignore file
├── etl.log                   # Log file (auto-generated)
├── etl_backup_YYYYMMDD.csv   # Daily data backup
├── requirements.txt          # Python dependencies
├── Retail_Etl_Mysql.py       # Main ETL script
├── LICENSE                   # MIT License for reuse
└── README.md                 # Project documentation
```

---

## Execution

To run the ETL pipeline:

```bash
python Retail_Etl_Mysql.py
```

---

## Note on Re-running the Pipeline

Each time you run `Retail_Etl_Mysql.py`, it will check and create the table if it doesn't exist, then insert the processed dataset.  
You can customize the script to handle deduplication or updates if needed.

---

## Contributing

Contributions are welcome!  
Feel free to fork the repository and submit a pull request for improvements, bug fixes, or feature suggestions.

---

## Author

Maintained by [Ankush Shukla](https://github.com/ankush0699)

---

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.








