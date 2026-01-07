# **⚽ Football Data Pipeline**
[![Version][Verison-Shield]]
Football Data Pipeline was a data engineering system to fetching, saving, and transforming football (in america it call soccer btw) data from 12 world famous competitions (including FIFA World Cup) into the ready-to-use data. 

# **Purpose**
This project was initiated to bring some points, including **:**
* Provide analytics team to get prepared football data from 12 leagues   
* Being my personal data engineering project

# **🛠️ Tech Stacks**
* [![Docker][Docker-Logo]][Docker-Url]
* [![Apache Airflow][Apache-Airflow-Logo]][Apache-Airflow-Url]
* [![Python][Python-Logo]][Python-Url]
* [![DBT][DBT-Logo]][DBT-Url]
* [![Postgres][Postgres-Logo]][Postgres-Url]

# **🔁 Data Fetch Flow**
Football Data API -> extract using python -> load into postgres -> transform into prepared data using dbt -->> containerized in Docker

<!--Markdown Links & Images-->
<!--Url-->
[Docker-Url]:https://www.docker.com/
[Apache-Airflow-Url]:https://airflow.apache.org/
[Python-Url]:https://www.python.org/
[DBT-Url]:https://www.getdbt.com/
[Postgres-Url]:https://www.postgresql.org/
<!--Logo-->
[Docker-Logo]:https://img.shields.io/badge/logo-Docker-blue?logo=docker&logoColor=ffffff
[Apache-Airflow-Logo]:https://img.shields.io/badge/logo-Apache_Airflow-blue?logo=apacheairflow&logoColor=ffffff
[Python-Logo]:https://img.shields.io/badge/logo-Python-blue?logo=python&logoColor=ffffff
[Postgres-Logo]:https://img.shields.io/badge/logo-Postgres-blue?logo=postgresql&logoColor=ffffff
[DBT-Logo]:https://www.shadcn.io/icon/logos-dbt
[Version-Shield]: https://img.shields.io/github/Version-V.1.0-brightgreen/style=for-the-badge