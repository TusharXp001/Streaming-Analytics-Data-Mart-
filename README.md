This project simulates a real-world streaming platform (like Netflix, Prime, or Disney+) and demonstrates how to build an end-to-end data pipeline using the Modern Data Stack.

The pipeline ingests raw streaming data into Google Cloud Storage (GCS), loads it into Snowflake, and transforms it with dbt into analytics-ready data marts. The marts can then be used for business intelligence dashboards and advanced analytics.

⚙️ Tech Stack
Google Cloud Storage (GCS) – raw data landing zone

Snowflake – cloud data warehouse

dbt (data build tool) – ELT transformations, testing, and data modeling

Power BI / Tableau / Looker Studio – for optional visualization

📂 Project Architecture
Raw Data in GCS

Users data (demographics, signup info)

Movies data (title, genre, language, release year)

Watch history (who watched what, when, and for how long)

Ratings (user ratings for movies)

Snowflake Raw Layer

Raw tables created from the ingested files

Data staged in its original form for reproducibility

dbt Transformations

Staging Models: Standardize column names, clean data formats

Intermediate Models: Join and enrich datasets

Data Marts: Final business-ready tables for analytics

User Mart – engagement metrics, segmentation

Content Mart – movie and genre performance

Engagement Mart – regional and device-level trends

Consumption Layer

Business users and analysts connect BI tools directly to Snowflake marts

Dashboards built for user behavior analysis, content strategy, and market insights

🖼 ER Diagram
The project is based on four core entities:

Users

Movies

Watch History

Ratings

Relationships:

A user can watch many movies (Users → Watch History)

A movie can be watched by many users (Movies → Watch History)

A user can rate many movies (Users → Ratings)

A movie can have many ratings (Movies → Ratings)

These raw entities are then transformed through dbt into structured marts for analysis.

🚀 Key Features
End-to-end pipeline from raw data ingestion to analytics-ready marts

Realistic streaming platform data simulation

dbt transformations with layered architecture (staging, intermediate, marts)

Built-in data testing and documentation through dbt

Cloud-native solution integrating GCS and Snowflake

📊 Example Insights
The marts enable insights such as:

Top genres by country

Most watched movies across platforms

Binge-watchers versus casual users

Device preferences for streaming

Highest rated versus most viewed movies

✅ Deliverables
Cloud data pipeline (GCS → Snowflake → dbt)

ER diagram of raw schema

Structured dbt models and marts

Analytics-ready datasets for BI tools

Optional dashboards demonstrating streaming insights

