Web Scraping to Kafka Mini-Pipeline 
Bonus Task 3 - Data Collection and Preparation

**Student ID:** 22B22B1510  
**Author:** Batyrbek Raiymbek  
**Target URL:** [Wikipedia - List of highest-grossing films](https://en.wikipedia.org/wiki/List_of_highest-grossing_films)


Project Overview
This project implements a complete data engineering mini-pipeline. It automates the extraction of movie data from a static Wikipedia page, performs multiple data cleaning transformations using Pandas and Regex, and streams the records into an **Apache Kafka** topic in JSON format.

Tech Stack
* **Language:** Python 3.12
* **Libraries:** `BeautifulSoup4`, `requests`, `pandas`, `kafka-python`, `re`
* **Infrastructure:** Apache Kafka (running via Docker)


Data Cleaning Process
The raw data scraped from Wikipedia is inconsistent. To ensure high-quality data for Kafka, the following **5 cleaning operations** were performed:

1. **Column Selection & Renaming:** Selected specific columns from the raw table and renamed them to `Rank`, `Title`, `Gross_USD`, and `Release_Year` for better readability.
2. **Regex Citation Removal:** Stripped Wikipedia reference tags (e.g., `[1]`, `[a]`) from movie titles and year strings using regular expressions.
3. **Numeric Transformation:** Cleaned the `Gross` column by removing currency symbols (`$`), suffixes (`B` for billions), and commas, converting the values to `float`.
4. **Year Normalization:** Extracted 4-digit integers from the date strings to standardize the `Release_Year` field.
5. **Quality Filtering:** Dropped rows with missing (NaN) or invalid numeric values to maintain data integrity.


How to Run

1. **Start Kafka Broker:**
   Ensure Kafka is running in Docker (container name: `22B22B1510_BONUS`).
2. **Start Consumer:**
   Run the following command to monitor the stream:
   ```bash
   docker exec -it 22B22B1510_BONUS /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic bonus_22B22B1510 --from-beginning

Execute Pipeline:
python script.py

Sample Kafka Message
Each record is pushed to the topic as a JSON object:
{
  "Rank": 1,
  "Title": "Avatar",
  "Gross_USD": 2923706026.0,
  "Release_Year": 2009
}

Output Files
script.py: The main logic for scraping, cleaning, and producing messages.

cleaned_data.csv: Local backup of the transformed dataset.
