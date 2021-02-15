#!/usr/bin/env python
# coding: utf-8

# # Exercise 03 - Columnar Vs Row Storage

# - The columnar storage extension used here: 
#     - cstore_fdw by citus_data [https://github.com/citusdata/cstore_fdw](https://github.com/citusdata/cstore_fdw)
# - The data tables are the ones used by citus_data to show the storage extension
# 

# In[1]:


get_ipython().run_line_magic('load_ext', 'sql')


# ## STEP 0 : Connect to the local database where Pagila is loaded
# 
# ### Create the database

# In[2]:


get_ipython().system("sudo -u postgres psql -c 'CREATE DATABASE reviews;'")

get_ipython().system('wget http://examples.citusdata.com/customer_reviews_1998.csv.gz')
get_ipython().system('wget http://examples.citusdata.com/customer_reviews_1999.csv.gz')

get_ipython().system('gzip -d customer_reviews_1998.csv.gz ')
get_ipython().system('gzip -d customer_reviews_1999.csv.gz ')

get_ipython().system('mv customer_reviews_1998.csv /tmp/customer_reviews_1998.csv')
get_ipython().system('mv customer_reviews_1999.csv /tmp/customer_reviews_1999.csv')


# ### Connect to the database

# In[3]:


DB_ENDPOINT = "127.0.0.1"
DB = 'reviews'
DB_USER = 'student'
DB_PASSWORD = 'student'
DB_PORT = '5432'

# postgresql://username:password@host:port/database
conn_string = "postgresql://{}:{}@{}:{}/{}"                         .format(DB_USER, DB_PASSWORD, DB_ENDPOINT, DB_PORT, DB)

print(conn_string)


# In[4]:


get_ipython().run_line_magic('sql', '$conn_string')


# ## STEP 1 :  Create a table with a normal  (Row) storage & load data
# 
# **TODO:** Create a table called customer_reviews_row with the column names contained in the `customer_reviews_1998.csv` and `customer_reviews_1999.csv` files.

# In[5]:


get_ipython().run_cell_magic('sql', '', 'DROP TABLE IF EXISTS customer_reviews_row;\nCREATE TABLE customer_reviews_row\n(\n    customer_id TEXT,\n    review_date DATE,\n    review_rating INTEGER,\n    review_votes INTEGER,\n    review_helpful_votes INTEGER,\n    product_id CHAR(10),\n    product_title TEXT,\n    product_sales_rank BIGINT,\n    product_group TEXT,\n    product_category TEXT,\n    product_subcategory TEXT,\n    similar_product_ids CHAR(10)[]\n)')


# **TODO:** Use the [COPY statement](https://www.postgresql.org/docs/9.2/sql-copy.html) to populate the tables with the data in the `customer_reviews_1998.csv` and `customer_reviews_1999.csv` files. You can access the files in the `/tmp/` folder.

# In[6]:


get_ipython().run_cell_magic('sql', '', "COPY customer_reviews_row FROM '/tmp/customer_reviews_1998.csv' WITH CSV;\nCOPY customer_reviews_row FROM '/tmp/customer_reviews_1999.csv' WITH CSV;")


# ## STEP 2 :  Create a table with columnar storage & load data
# 
# First, load the extension to use columnar storage in Postgres.

# In[7]:


get_ipython().run_cell_magic('sql', '', '\n-- load extension first time after install\nCREATE EXTENSION cstore_fdw;\n\n-- create server object\nCREATE SERVER cstore_server FOREIGN DATA WRAPPER cstore_fdw;')


# **TODO:** Create a `FOREIGN TABLE` called `customer_reviews_col` with the column names contained in the `customer_reviews_1998.csv` and `customer_reviews_1999.csv` files.

# In[8]:


get_ipython().run_cell_magic('sql', '', "-- create foreign table\nDROP FOREIGN TABLE IF EXISTS customer_reviews_col;\n\n-------------\nCREATE FOREIGN TABLE customer_reviews_col\n(\n    customer_id TEXT,\n    review_date DATE,\n    review_rating INTEGER,\n    review_votes INTEGER,\n    review_helpful_votes INTEGER,\n    product_id CHAR(10),\n    product_title TEXT,\n    product_sales_rank BIGINT,\n    product_group TEXT,\n    product_category TEXT,\n    product_subcategory TEXT,\n    similar_product_ids CHAR(10)[]\n)\n\n\n-------------\n-- leave code below as is\nSERVER cstore_server\nOPTIONS(compression 'pglz');")


# **TODO:** Use the [COPY statement](https://www.postgresql.org/docs/9.2/sql-copy.html) to populate the tables with the data in the `customer_reviews_1998.csv` and `customer_reviews_1999.csv` files. You can access the files in the `/tmp/` folder.

# In[9]:


get_ipython().run_cell_magic('sql', '', "COPY customer_reviews_col FROM '/tmp/customer_reviews_1998.csv' WITH CSV;\nCOPY customer_reviews_col FROM '/tmp/customer_reviews_1999.csv' WITH CSV;")


# ## Step 3: Compare perfromamce
# 
# Now run the same query on the two tables and compare the run time. Which form of storage is more performant?
# 
# **TODO**: Write a query that calculates the average `review_rating` by `product_title` for all reviews in 1995. Sort the data by `review_rating` in descending order. Limit the results to 20.
# 
# First run the query on `customer_reviews_row`:

# In[18]:


get_ipython().run_cell_magic('time', '', "%%sql\n\nSELECT product_title, avg(review_rating) as average_rating\nFROM customer_reviews_row\nWHERE EXTRACT(YEAR FROM review_date) = '1995'\nGROUP BY review_rating,product_title\nORDER BY review_rating DESC\nlimit 20;")


#  Then on `customer_reviews_col`:

# In[20]:


get_ipython().run_cell_magic('time', '', "%%sql\n\nSELECT product_title, avg(review_rating) as average_rating\nFROM customer_reviews_col\nWHERE EXTRACT(YEAR FROM review_date) = '1995'\nGROUP BY review_rating,product_title\nORDER BY review_rating DESC\nlimit 20;")


# ## Conclusion: We can see that the columnar storage is faster!

# In[21]:


get_ipython().run_cell_magic('time', '', "%%sql\nSELECT\n    customer_id, review_date, review_rating, product_id, product_title\nFROM\n    customer_reviews_row\nWHERE\n    customer_id ='A27T7HVDXA3K2A' AND\n    product_title LIKE '%Dune%' AND\n    review_date >= '1998-01-01' AND\n    review_date <= '1998-12-31';")


# In[22]:


get_ipython().run_cell_magic('time', '', "%%sql\nSELECT\n    customer_id, review_date, review_rating, product_id, product_title\nFROM\n    customer_reviews_col\nWHERE\n    customer_id ='A27T7HVDXA3K2A' AND\n    product_title LIKE '%Dune%' AND\n    review_date >= '1998-01-01' AND\n    review_date <= '1998-12-31';")


# In[23]:


get_ipython().run_cell_magic('time', '', "%%sql\nSELECT product_title, avg(review_rating)\nFROM customer_reviews_col\nWHERE review_date >= '1995-01-01' \n    AND review_date <= '1998-12-31'\nGROUP BY product_title\nORDER by product_title\nLIMIT 20;")


# In[24]:


get_ipython().run_cell_magic('time', '', "%%sql\nSELECT product_title, avg(review_rating)\nFROM customer_reviews_row\nWHERE review_date >= '1995-01-01' \n    AND review_date <= '1998-12-31'\nGROUP BY product_title\nORDER by product_title\nLIMIT 20;")


# In[ ]:




