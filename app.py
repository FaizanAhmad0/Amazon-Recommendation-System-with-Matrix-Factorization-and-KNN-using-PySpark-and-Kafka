from flask import Flask, render_template, request, redirect, url_for,session
import random
import string
import dask.dataframe as dd
import pickle
import pandas as pd
import numpy as np
from sklearn.neighbors import NearestNeighbors
from pyspark.sql.functions import col, mean, stddev, count
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from kafka import KafkaProducer

from kafka import KafkaConsumer






database_uri = "mongodb://127.0.0.1:27017"
working_directory = "C://Working Directory/*"
spark = SparkSession.builder.appName("myApp") \
    .config("spark.mongodb.input.uri", database_uri) \
    .config("spark.mongodb.output.uri", database_uri) \
    .config('spark.driver.extraClassPath', working_directory) \
    .config("spark.driver.memory", "4g") \
    .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true") \
    .getOrCreate()
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

consumer = KafkaConsumer(
    'new',  # Subscribe to the 'cart' topic
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',  # Read from the beginning of the topic
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: x.decode('utf-8')  # Deserialize the message value
)

def load_data(pickle_file,item):
    # Load encoded pandas DataFrame from a pickle file
    with open(pickle_file, 'rb') as f:
        encoded_pandas_df = pickle.load(f)

    # Convert the Pandas DataFrame to a Dask DataFrame
    dask_df = dd.from_pandas(encoded_pandas_df, npartitions=4)

    # Categorize the columns
    dask_df = dask_df.categorize(columns=['reviewerID', 'asin'])

    # Perform label encoding using Dask
    dask_df['reviewerID_idx'] = dask_df['reviewerID'].cat.codes
    dask_df['asin_idx'] = dask_df['asin'].cat.codes

    # Compute and display the Dask DataFrame (convert it back to Pandas)
    encoded_pandas_df = dask_df.compute()


    spark.conf.set("spark.sql.execution.arrow.enabled","true")
    data=spark.createDataFrame(encoded_pandas_df) 
    (training, test) = data.randomSplit([0.8, 0.2])

    als = ALS(maxIter=5, regParam=0.01, userCol="reviewerID_idx", itemCol="asin_idx", ratingCol="overall",
          coldStartStrategy="drop", nonnegative=True)
    print(1)
# Train the model on the training set
    model = als.fit(training)
    # Filter and get distinct asin values
    product_id = item
    product_idx = data.filter(F.col("asin") == product_id).select("asin_idx").collect()[0][0]
    item_factors = model.itemFactors.toPandas()
    item_factors = item_factors.set_index("id")
    item_factors=item_factors.reset_index()
    for i in range(10):
        item_factors[f'feature_{i}'] = item_factors['features'].apply(lambda x: x[i])
    item_factors= item_factors.drop('features', axis=1)
    df=item_factors

    nn = NearestNeighbors(n_neighbors=11, algorithm="ball_tree")
    nn.fit(df)
    indices = nn.kneighbors(df.loc[product_idx].values.reshape(1, -1))[1][0]
    similar_products = df.iloc[indices[1:], :]

    temp=similar_products['id']
    temp=pd.DataFrame(similar_products['id'])
    pandas_df=temp

    pandas_df = spark.createDataFrame(pandas_df)

    df_filtered = pandas_df.join(data, pandas_df.id == data.asin_idx, 'inner').select(col('id'), col('asin'))

    distinct_asin = df_filtered.select("asin").distinct()

    distinct_asin_list = [row['asin'] for row in distinct_asin.collect()]
    distinct_asin_array = np.array(distinct_asin_list)

    return distinct_asin_array

app = Flask(__name__)
app.secret_key = 'iamX2'
# Define some example products
# PRODUCTS = [
#     {'id': 1, 'name': 'B017OBSCOS', 'price': 300.0, 'description': 'Free SSd For Full Marks get 3 for all the demo Takers', 'image': 'https://myshop.pk/pub/media/catalog/product/cache/26f8091d81cea4b38d820a1d1a4f62be/t/r/transcend-ssd230s-250gb-ssd-myshop-pk-1.jpg'},
#     {'id': 2, 'name': 'B015H1YNV8', 'price': 1500.0, 'description': 'Best Machine in the world PS:Dr Kifayat ka laptop ', 'image': 'data:image/jpeg;base64,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'},
#     {'id': 3, 'name': 'B017O9P72A', 'price': 300.0, 'description': 'Buy 1 get 1 Free', 'image': 'https://myshop.pk/pub/media/catalog/product/cache/26f8091d81cea4b38d820a1d1a4f62be/t/r/transcend-ssd230s-250gb-ssd-myshop-pk-1.jpg'},
# ]
PRODUCTS_ALL = [
    {'id': 1, 'name': 'B017OBI3AQ', 'price': 300.0, 'description': 'Free SSd ', 'image': 'https://myshop.pk/pub/media/catalog/product/cache/26f8091d81cea4b38d820a1d1a4f62be/t/r/transcend-ssd230s-250gb-ssd-myshop-pk-1.jpg'},
    {'id': 2, 'name': 'B015H1YNV8', 'price': 1500.0, 'description': 'Best Machine  ', 'image': 'dNJqAbYCozCHTSaAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA//9k='},
    {'id': 3, 'name': 'B017O9P72A', 'price': 300.0, 'description': 'Really Needed in this Project', 'image': 'https://myshop.pk/pub/media/catalog/product/cache/26f8091d81cea4b38d820a1d1a4f62be/t/r/transcend-ssd230s-250gb-ssd-myshop-pk-1.jpg'},
    {'id': 4, 'name': 'B017OBJI46', 'price': 700.0, 'description': 'Efficient and Powerful Machine', 'image': 'https://images.priceoye.pk/apple-iphone-14-pakistan-priceoye-3j7db.jpg'},
    {'id': 5, 'name': 'B017OBZR50', 'price': 450.0, 'description': 'Lightweight and Portable Machine', 'image': 'https://laptopmall.pk/storage/media/OMniSc3EonMThhdii9KySkPDooXqo5nQnKazBpYp.png'},
    {'id': 6, 'name': 'B017OBIKIG', 'price': 1200.0, 'description': 'High-End Gaming Machine', 'image': 'https://m.media-amazon.com/images/M/MV5BNWM4MjE1YTItMDFjOC00MDBiLWE2ZGYtNTU2Y2IwYzhhOGJjXkEyXkFqcGdeQXVyMTA0MTM5NjI2._V1_.jpg'}
    # Add more products here
]

PRODUCTS = random.sample(PRODUCTS_ALL, 3)
# Define a list of recommendations
#RECOMMENDATIONS = ["A1", "A2", "A3", "A4"]
RECOMMENDATIONS = load_data('encoded_pandas_df.pickle','B017OBSCOS').tolist()#[''.join(random.choices(string.ascii_letters, k=10)) for _ in range(5)]  # Update the RECOMMENDATIONS list with the first 5 available recommendations


# Define a simple shopping cart as a list of products
cart = []



# @app.route('/login', methods=['GET', 'POST'])
# def login():
#     if request.method == 'POST':
#         # Check if the username and password are correct
#         if request.form.get('username') == 'myusername@gmail.com' and request.form.get('password') == 'mypassword':
#             # Set a session variable to indicate the user is logged in
#             session['logged_in'] = True
#             return redirect(url_for('index'))
#         else:
#             return render_template('login.html')
#     else:
#         return render_template('login.html')

# Define a function to calculate the total price of the items in the cart
def calculate_cart_total():
    return sum(item['price'] * item['qty'] for item in cart)


# Define a function to update the list of available recommendations based on the contents of the cart
def update_recommendations():
    global RECOMMENDATIONS  # Use the global RECOMMENDATIONS variable
    cart_items = [item['name'] for item in cart]
    available_recommendations = [recommendation for recommendation in RECOMMENDATIONS if recommendation not in cart_items]
    random.shuffle(available_recommendations)  # Shuffle the list of available recommendations
    if len(cart) != 0:
        # Read the latest message from the 'cart' topic
        polled_messages = consumer.poll()
        for tp, messages in polled_messages.items():
            for message in messages:
                name_value = message.value  # Get the value of the message
                consumer.commit()  # Commit the offset of the message
                
                # Update the list of available recommendations based on the contents of the cart
                RECOMMENDATIONS = load_data('encoded_pandas_df.pickle', name_value).tolist()
    else:
        # Update the list of available recommendations with the default values
        RECOMMENDATIONS = load_data('encoded_pandas_df.pickle', 'B017OBSCOS').tolist()


# @app.route('/')
# def index():
#     cart_total = calculate_cart_total()
#     update_recommendations()  # Update the list of available recommendations
#     return render_template('index.html', products=PRODUCTS, recommendations=RECOMMENDATIONS, cart=cart, cart_total=cart_total)



@app.route('/add_to_cart', methods=['POST'])
def add_to_cart():
    product_id = int(request.form['product_id'])
    product = next((p for p in PRODUCTS if p['id'] == product_id), None)
    if product:
        # If the product is already in the cart, increment the quantity
        for item in cart:
            if item['id'] == product_id:
                item['qty'] += 1
                break
        else:
            # Otherwise, add the product to the cart with quantity 1
            cart.append({'id': product_id, 'name': product['name'], 'price': product['price'], 'qty': 1})
        
        # Send message to Kafka
        try:
            producer.send('new', value=product["name"].encode('utf-8'))
            producer.flush()
        except Exception as e:
            print("Error sending message to Kafka:", e)
    
    return redirect(url_for('index'))

@app.route('/')
def index():
    if 'logged_in' in session and session['logged_in']:
        cart_total = calculate_cart_total()
        update_recommendations()  # Update the list of available recommendations
        return render_template('index.html', products=PRODUCTS, recommendations=RECOMMENDATIONS, cart=cart, cart_total=cart_total)
    else:
        return render_template('login.html')



@app.route('/thankyou',methods = ['GET'])
def thankyou():
    return render_template('thankyou.html')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)


