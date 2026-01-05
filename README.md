# **⚽ Football Data Pipeline**

Football Data Pipeline was a data engineering system to fetching, saving, and transforming football (in america it call soccer btw) data from 12 world famous competitions (including FIFA World Cup) into the ready-to-use data. This project was built to :
* football analytics purpose
* make decisions and predictions from football data

# **📖 Code Guideline**
* File Naming : Snake Case (insert_records.py)
* Folder Naming : Fishbone (football-data-pipeline)
* Variable Naming : Snake Case (teams_data)
* Functions Naming : Snake Case (connect_to_db())

# **🛠️ Tech Stacks**
<div align="left">
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon@latest/icons/docker/docker-plain-wordmark.svg" height="40" alt="docker-logo"/>
  <img width="12"/>
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon@latest/icons/apacheairflow/apacheairflow-original-wordmark.svg" height="40" alt="apache-airflow-logo"/>
  <img width="12"/>
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon@latest/icons/python/python-original.svg" height="40" alt="python-logo"/>
  <img width="12"/>
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon@latest/icons/postgresql/postgresql-original.svg" height="40" alt="postgres-logo"/>
</div>

# **🔁 Data Fetch Flow**
Football Data API -> extract using python -> load into postgres -> transform into prepared data using dbt -->> containerized in Docker

