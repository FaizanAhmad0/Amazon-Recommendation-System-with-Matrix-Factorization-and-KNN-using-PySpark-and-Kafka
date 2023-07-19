# Amazon Recommendation System with Matrix Factorization and KNN

# Description:
Our Amazon Recommendation System project aims to build a robust recommendation model for personalized product recommendations based on customer purchase history. We started by uploading a massive dataset of 118 GB containing Amazon customer and product purchase information. To handle such a substantial dataset efficiently, we leveraged PySpark to perform data processing and analysis.

The project workflow began with establishing a PySpark session, enabling us to process the data quickly and efficiently. We carefully dropped unnecessary data to streamline the analysis. Exploratory Data Analysis (EDA) was conducted to gain insights into the dataset, identifying patterns and trends in customer behavior.

To optimize the data preprocessing phase, we incorporated Dask, which significantly sped up the encoding and preprocessing steps. Dask's parallel computing capabilities made it possible to handle large-scale data processing more effectively.

One of the major challenges we faced was dealing with the sparse matrix of such a massive dataset, which proved to be time-consuming. To address this issue, we implemented Matrix Factorization using the Alternating Least Squares (ALS) library, a powerful technique to factorize large sparse matrices and provide better recommendations.Additionally, we employed the K-Nearest Neighbors (KNN) algorithm to create an item-based recommendation model, offering customers product suggestions based on similar items.

To overcome the cold start problem, where new users have no purchase history, we introduced a strategy to display the best-selling items, ensuring a better user experience for new customers.

To ensure real-time updates of recommendations, we integrated Kafka, a distributed streaming platform. Kafka facilitated the flow of live data to the consumer and dynamically updated recommendations on our Flask web app.

Throughout the project, we encountered transitions between Dask and PySpark datasets. To facilitate this seamless switch, we adopted the Arrow library, which streamlined data conversion and transfer between the two frameworks.
Our Amazon Recommendation System project combines the power of PySpark, Dask, Matrix Factorization, KNN, Kafka, and Flask to deliver a scalable, efficient, and accurate recommendation system for Amazon's diverse customer base.

# Requirements
As this project was deployed on windows you will need the following things.

Python
MongoDB 
+ mongo-java-driver
+ mongo-spark-connector
  Note: You have to keep these files in a folder named Working Directory and the path to it is mentioned in the Spark-Session at the start of the code.
PySpark
Dask
Arrow
Kafka (You can follow any video to download it on windows)
Flask



# How To Run:
First step is to run kafka producer and consumer.
# These commands are specifically for windows

# Start Zookeeper
`.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties`

# Start Kafka Server
`.\bin\windows\kafka-server-start.bat .\config\server.properties`

# Create a topic
`kafka-topics.bat --create --bootstrap-server localhost:9092 --topic test`
Note: The topic should be the same as you mentioned in your producer and consumer

# Run Producer 
`kafka-console-producer.bat --broker-list localhost:9092 --topic test`

# Run Consumer
`kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic test`

On VS Code Terminal:
`python run app.py`

You will get the link to the web app.

NOTE: The Focus of this project was to buid a recommendation model on large data using big data techniques.

