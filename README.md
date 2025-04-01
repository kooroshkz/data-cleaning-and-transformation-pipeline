# Data Cleaning and Transformation Pipeline

This project allows you to clean and transform datasets from CSV and JSON files using Apache Spark.

## Requirements

- **Java 11** or later
- **Apache Spark 3.x**
- **SBT** (Scala Build Tool)

## Setup

### 1. Clone the Repository

```bash
git clone https://github.com/kooroshkz/data-cleaning-and-transformation-pipeline.git
cd data-cleaning-and-transformation-pipeline
```

### 2. Install Dependencies

Run the following to install necessary dependencies:

```bash
sbt clean update
```

## How to Use

### Option 1: Using Default Data Folder

1. Place your CSV/JSON files in the `data/` folder.
2. Run the pipeline using:

```bash
sbt run
```

The program will automatically pick up the dataset from the `data/` folder, clean it, and save the output as `original_filename_cleaned.csv`.

### Option 2: Specify a File

If you want to clean a specific file, you can run the following:

```bash
sbt run /path/to/your/file.csv
```

The program will clean the specific file and save the result as `file_cleaned.csv`.

### Handling Multiple Files

If there are multiple CSV/JSON files in the `data/` folder, the program will ask you to choose which file you want to clean. If only one file is found, it will process that file automatically.

## What It Does

1. **Loads data**: It can load both CSV and JSON files.
2. **Fills missing values**: For numeric columns (e.g., `Age`, `Fare`), missing values are filled with the mean of that column.
3. **Converts data types**: It converts columns like `Age` and `Fare` into numeric types (`double`).
4. **Saves cleaned data**: The cleaned dataset is saved in CSV format, named as `original_filename_cleaned.csv`.

### Example

For example, if you have `data/titanic.csv`, the program will clean the data and save it as `data/titanic_cleaned.csv`.

## File Structure

```plaintext
.
├── data/                 # Folder containing CSV/JSON files
│   ├── titanic.csv       # Example dataset (CSV)
│   └── another_file.json # Another example dataset (JSON)
├── src/                  # Source code
│   ├── main/scala/       # Scala files for data processing
├── target/               # Output folder for compiled code and results
└── build.sbt             # Build configuration
```
