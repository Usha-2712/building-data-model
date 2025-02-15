```python
!pip install psycopg2
```

    Collecting psycopg2
      Downloading psycopg2-2.9.10-cp312-cp312-win_amd64.whl.metadata (5.0 kB)
    Downloading psycopg2-2.9.10-cp312-cp312-win_amd64.whl (1.2 MB)
       ---------------------------------------- 0.0/1.2 MB ? eta -:--:--
       ---------------------------------------- 0.0/1.2 MB ? eta -:--:--
       --- ------------------------------------ 0.1/1.2 MB 1.6 MB/s eta 0:00:01
       ---------- ----------------------------- 0.3/1.2 MB 2.7 MB/s eta 0:00:01
       ---------------- ----------------------- 0.5/1.2 MB 2.9 MB/s eta 0:00:01
       ----------------------- ---------------- 0.7/1.2 MB 3.4 MB/s eta 0:00:01
       --------------------------- ------------ 0.8/1.2 MB 3.6 MB/s eta 0:00:01
       --------------------------------- ------ 1.0/1.2 MB 3.4 MB/s eta 0:00:01
       ---------------------------------------  1.2/1.2 MB 3.7 MB/s eta 0:00:01
       ---------------------------------------- 1.2/1.2 MB 3.4 MB/s eta 0:00:00
    Installing collected packages: psycopg2
    Successfully installed psycopg2-2.9.10
    

    
    [notice] A new release of pip is available: 24.0 -> 25.0.1
    [notice] To update, run: python.exe -m pip install --upgrade pip
    


```python
import psycopg2
```


```python
try:
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres password=12345678")
except psycopg2.Error as e:
    print("Error: could not make connection to the postgres database")
    print(e)
```


```python
try:
    cur = conn.cursor()
except psycopg2.Error as e:
    print("Error: could not get cursor to the database")
    print(e)
```


```python
conn.set_session(autocommit=True)
```


```python
try:
    cur.execute("create database myfirstdb")
except psycopg2.Error as e:
    print(e)
```

    database "myfirstdb" already exists
    
    


```python
try:
    conn.close()
except psycopg2.Error as e:
    print(e)

try:
    conn = psycopg2.connect("host=localhost dbname=myfirstdb user=postgres password=12345678")
except psycopg2.Error as e:
    print("Error: could not make connection to the postgres database")
    print(e)

try:
    cur = conn.cursor()
except psycopg2.Error as e:
    print("Error: could not get cursor to the database")
    print(e)

conn.set_session(autocommit=True)
```


```python
try:
    cur.execute("CREATE TABLE IF NOT EXISTS students (student_id int, name varchar,\
    age int, gender varchar, subject varchar, marks int);")
except psycopg2.Error as e:
    print("Error: Issue creating table")
    print(e)
```


```python
try:
    cur.execute("INSERT INTO students (student_id, name, age, gender, subject, marks)\
                VALUES(%s, %s, %s, %s, %s, %s)",\
                (1, "Raj", 23, "Male", "Python", 85))
except psycopg2.Error as e:
    print("Error:Inserting Rows")
    print (e)

try:
    cur.execute("INSERT INTO students (student_id, name, age, gender, subject, marks)\
                VALUES(%s, %s, %s, %s, %s, %s)",\
                (2, "Priya", 22, "Female", "Python", 86))
except psycopg2.Error as e:
    print("Error:Inserting Rows")
    print (e)
```


```python
try:
    cur.execute("SELECT * FROM students;")
except psycopg2.Error as e:
    print("Error: select *")
    print(e)

row= cur.fetchone()
while row:
    print(row)
    row=cur.fetchone()
```

    (1, 'Raj', 23, 'Male', 'Python', 85)
    (2, 'Priya', 22, 'Female', 'Python', 86)
    

cur.close()
conn.close()


```python

```


```python
import psycopg2
import pandas as pd
```


```python
def create_database():
    #connect to default database
    #conn = psycopg2.connect("hostlocalhost dbname=studentdb user=student password=12345678")
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres password=12345678")
    conn.set_session(autocommit=True)
    cur=conn.cursor()

    #create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS creditscore")
    cur.execute("CREATE DATABASE creditscore")

    #close connection to default database
    conn.close()

    #connect to sparkify database
    conn = psycopg2.connect("host=localhost dbname=creditscore user=postgres password=12345678")
    cur = conn.cursor()

    return cur,conn
    
```


```python
def drop_tables(cur,conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
```


```python
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
```


```python
CreditScoreTest = pd.read_csv("test.csv")
```


```python
CreditScoreTest.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ID</th>
      <th>Customer_ID</th>
      <th>Month</th>
      <th>Name</th>
      <th>Age</th>
      <th>SSN</th>
      <th>Occupation</th>
      <th>Annual_Income</th>
      <th>Monthly_Inhand_Salary</th>
      <th>Num_Bank_Accounts</th>
      <th>...</th>
      <th>Num_Credit_Inquiries</th>
      <th>Credit_Mix</th>
      <th>Outstanding_Debt</th>
      <th>Credit_Utilization_Ratio</th>
      <th>Credit_History_Age</th>
      <th>Payment_of_Min_Amount</th>
      <th>Total_EMI_per_month</th>
      <th>Amount_invested_monthly</th>
      <th>Payment_Behaviour</th>
      <th>Monthly_Balance</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0x160a</td>
      <td>CUS_0xd40</td>
      <td>September</td>
      <td>Aaron Maashoh</td>
      <td>23</td>
      <td>821-00-0265</td>
      <td>Scientist</td>
      <td>19114.12</td>
      <td>1824.843333</td>
      <td>3</td>
      <td>...</td>
      <td>2022.0</td>
      <td>Good</td>
      <td>809.98</td>
      <td>35.030402</td>
      <td>22 Years and 9 Months</td>
      <td>No</td>
      <td>49.574949</td>
      <td>236.64268203272135</td>
      <td>Low_spent_Small_value_payments</td>
      <td>186.26670208571772</td>
    </tr>
    <tr>
      <th>1</th>
      <td>0x160b</td>
      <td>CUS_0xd40</td>
      <td>October</td>
      <td>Aaron Maashoh</td>
      <td>24</td>
      <td>821-00-0265</td>
      <td>Scientist</td>
      <td>19114.12</td>
      <td>1824.843333</td>
      <td>3</td>
      <td>...</td>
      <td>4.0</td>
      <td>Good</td>
      <td>809.98</td>
      <td>33.053114</td>
      <td>22 Years and 10 Months</td>
      <td>No</td>
      <td>49.574949</td>
      <td>21.465380264657146</td>
      <td>High_spent_Medium_value_payments</td>
      <td>361.44400385378196</td>
    </tr>
    <tr>
      <th>2</th>
      <td>0x160c</td>
      <td>CUS_0xd40</td>
      <td>November</td>
      <td>Aaron Maashoh</td>
      <td>24</td>
      <td>821-00-0265</td>
      <td>Scientist</td>
      <td>19114.12</td>
      <td>1824.843333</td>
      <td>3</td>
      <td>...</td>
      <td>4.0</td>
      <td>Good</td>
      <td>809.98</td>
      <td>33.811894</td>
      <td>NaN</td>
      <td>No</td>
      <td>49.574949</td>
      <td>148.23393788500925</td>
      <td>Low_spent_Medium_value_payments</td>
      <td>264.67544623342997</td>
    </tr>
    <tr>
      <th>3</th>
      <td>0x160d</td>
      <td>CUS_0xd40</td>
      <td>December</td>
      <td>Aaron Maashoh</td>
      <td>24_</td>
      <td>821-00-0265</td>
      <td>Scientist</td>
      <td>19114.12</td>
      <td>NaN</td>
      <td>3</td>
      <td>...</td>
      <td>4.0</td>
      <td>Good</td>
      <td>809.98</td>
      <td>32.430559</td>
      <td>23 Years and 0 Months</td>
      <td>No</td>
      <td>49.574949</td>
      <td>39.08251089460281</td>
      <td>High_spent_Medium_value_payments</td>
      <td>343.82687322383634</td>
    </tr>
    <tr>
      <th>4</th>
      <td>0x1616</td>
      <td>CUS_0x21b1</td>
      <td>September</td>
      <td>Rick Rothackerj</td>
      <td>28</td>
      <td>004-07-5839</td>
      <td>_______</td>
      <td>34847.84</td>
      <td>3037.986667</td>
      <td>2</td>
      <td>...</td>
      <td>5.0</td>
      <td>Good</td>
      <td>605.03</td>
      <td>25.926822</td>
      <td>27 Years and 3 Months</td>
      <td>No</td>
      <td>18.816215</td>
      <td>39.684018417945296</td>
      <td>High_spent_Large_value_payments</td>
      <td>485.2984336755923</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 27 columns</p>
</div>




```python
CreditScoreTest_clean = CreditScoreTest[['ID', 'Customer_ID', 'Month', 'Name', 'Age', 'SSN', 'Occupation']]
```


```python
CreditScoreTest_clean.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ID</th>
      <th>Customer_ID</th>
      <th>Month</th>
      <th>Name</th>
      <th>Age</th>
      <th>SSN</th>
      <th>Occupation</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0x160a</td>
      <td>CUS_0xd40</td>
      <td>September</td>
      <td>Aaron Maashoh</td>
      <td>23</td>
      <td>821-00-0265</td>
      <td>Scientist</td>
    </tr>
    <tr>
      <th>1</th>
      <td>0x160b</td>
      <td>CUS_0xd40</td>
      <td>October</td>
      <td>Aaron Maashoh</td>
      <td>24</td>
      <td>821-00-0265</td>
      <td>Scientist</td>
    </tr>
    <tr>
      <th>2</th>
      <td>0x160c</td>
      <td>CUS_0xd40</td>
      <td>November</td>
      <td>Aaron Maashoh</td>
      <td>24</td>
      <td>821-00-0265</td>
      <td>Scientist</td>
    </tr>
    <tr>
      <th>3</th>
      <td>0x160d</td>
      <td>CUS_0xd40</td>
      <td>December</td>
      <td>Aaron Maashoh</td>
      <td>24_</td>
      <td>821-00-0265</td>
      <td>Scientist</td>
    </tr>
    <tr>
      <th>4</th>
      <td>0x1616</td>
      <td>CUS_0x21b1</td>
      <td>September</td>
      <td>Rick Rothackerj</td>
      <td>28</td>
      <td>004-07-5839</td>
      <td>_______</td>
    </tr>
  </tbody>
</table>
</div>




```python
CreditScoreTrain = pd.read_csv("train.csv") 

```

    C:\Users\Usha Lalam\AppData\Local\Temp\ipykernel_14164\743205147.py:1: DtypeWarning: Columns (26) have mixed types. Specify dtype option on import or set low_memory=False.
      CreditScoreTrain = pd.read_csv("train.csv")
    


```python
CreditScoreTrain.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ID</th>
      <th>Customer_ID</th>
      <th>Month</th>
      <th>Name</th>
      <th>Age</th>
      <th>SSN</th>
      <th>Occupation</th>
      <th>Annual_Income</th>
      <th>Monthly_Inhand_Salary</th>
      <th>Num_Bank_Accounts</th>
      <th>...</th>
      <th>Credit_Mix</th>
      <th>Outstanding_Debt</th>
      <th>Credit_Utilization_Ratio</th>
      <th>Credit_History_Age</th>
      <th>Payment_of_Min_Amount</th>
      <th>Total_EMI_per_month</th>
      <th>Amount_invested_monthly</th>
      <th>Payment_Behaviour</th>
      <th>Monthly_Balance</th>
      <th>Credit_Score</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0x1602</td>
      <td>CUS_0xd40</td>
      <td>January</td>
      <td>Aaron Maashoh</td>
      <td>23</td>
      <td>821-00-0265</td>
      <td>Scientist</td>
      <td>19114.12</td>
      <td>1824.843333</td>
      <td>3</td>
      <td>...</td>
      <td>_</td>
      <td>809.98</td>
      <td>26.822620</td>
      <td>22 Years and 1 Months</td>
      <td>No</td>
      <td>49.574949</td>
      <td>80.41529543900253</td>
      <td>High_spent_Small_value_payments</td>
      <td>312.49408867943663</td>
      <td>Good</td>
    </tr>
    <tr>
      <th>1</th>
      <td>0x1603</td>
      <td>CUS_0xd40</td>
      <td>February</td>
      <td>Aaron Maashoh</td>
      <td>23</td>
      <td>821-00-0265</td>
      <td>Scientist</td>
      <td>19114.12</td>
      <td>NaN</td>
      <td>3</td>
      <td>...</td>
      <td>Good</td>
      <td>809.98</td>
      <td>31.944960</td>
      <td>NaN</td>
      <td>No</td>
      <td>49.574949</td>
      <td>118.28022162236736</td>
      <td>Low_spent_Large_value_payments</td>
      <td>284.62916249607184</td>
      <td>Good</td>
    </tr>
    <tr>
      <th>2</th>
      <td>0x1604</td>
      <td>CUS_0xd40</td>
      <td>March</td>
      <td>Aaron Maashoh</td>
      <td>-500</td>
      <td>821-00-0265</td>
      <td>Scientist</td>
      <td>19114.12</td>
      <td>NaN</td>
      <td>3</td>
      <td>...</td>
      <td>Good</td>
      <td>809.98</td>
      <td>28.609352</td>
      <td>22 Years and 3 Months</td>
      <td>No</td>
      <td>49.574949</td>
      <td>81.699521264648</td>
      <td>Low_spent_Medium_value_payments</td>
      <td>331.2098628537912</td>
      <td>Good</td>
    </tr>
    <tr>
      <th>3</th>
      <td>0x1605</td>
      <td>CUS_0xd40</td>
      <td>April</td>
      <td>Aaron Maashoh</td>
      <td>23</td>
      <td>821-00-0265</td>
      <td>Scientist</td>
      <td>19114.12</td>
      <td>NaN</td>
      <td>3</td>
      <td>...</td>
      <td>Good</td>
      <td>809.98</td>
      <td>31.377862</td>
      <td>22 Years and 4 Months</td>
      <td>No</td>
      <td>49.574949</td>
      <td>199.4580743910713</td>
      <td>Low_spent_Small_value_payments</td>
      <td>223.45130972736786</td>
      <td>Good</td>
    </tr>
    <tr>
      <th>4</th>
      <td>0x1606</td>
      <td>CUS_0xd40</td>
      <td>May</td>
      <td>Aaron Maashoh</td>
      <td>23</td>
      <td>821-00-0265</td>
      <td>Scientist</td>
      <td>19114.12</td>
      <td>1824.843333</td>
      <td>3</td>
      <td>...</td>
      <td>Good</td>
      <td>809.98</td>
      <td>24.797347</td>
      <td>22 Years and 5 Months</td>
      <td>No</td>
      <td>49.574949</td>
      <td>41.420153086217326</td>
      <td>High_spent_Medium_value_payments</td>
      <td>341.48923103222177</td>
      <td>Good</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 28 columns</p>
</div>




```python
CreditScoreTrain_clean = CreditScoreTrain[['ID', 'Customer_ID', 'Month', 'Name', 'Age', 'SSN', 'Occupation','Annual_Income']]
```


```python
CreditScoreTrain_clean.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ID</th>
      <th>Customer_ID</th>
      <th>Month</th>
      <th>Name</th>
      <th>Age</th>
      <th>SSN</th>
      <th>Occupation</th>
      <th>Annual_Income</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0x1602</td>
      <td>CUS_0xd40</td>
      <td>January</td>
      <td>Aaron Maashoh</td>
      <td>23</td>
      <td>821-00-0265</td>
      <td>Scientist</td>
      <td>19114.12</td>
    </tr>
    <tr>
      <th>1</th>
      <td>0x1603</td>
      <td>CUS_0xd40</td>
      <td>February</td>
      <td>Aaron Maashoh</td>
      <td>23</td>
      <td>821-00-0265</td>
      <td>Scientist</td>
      <td>19114.12</td>
    </tr>
    <tr>
      <th>2</th>
      <td>0x1604</td>
      <td>CUS_0xd40</td>
      <td>March</td>
      <td>Aaron Maashoh</td>
      <td>-500</td>
      <td>821-00-0265</td>
      <td>Scientist</td>
      <td>19114.12</td>
    </tr>
    <tr>
      <th>3</th>
      <td>0x1605</td>
      <td>CUS_0xd40</td>
      <td>April</td>
      <td>Aaron Maashoh</td>
      <td>23</td>
      <td>821-00-0265</td>
      <td>Scientist</td>
      <td>19114.12</td>
    </tr>
    <tr>
      <th>4</th>
      <td>0x1606</td>
      <td>CUS_0xd40</td>
      <td>May</td>
      <td>Aaron Maashoh</td>
      <td>23</td>
      <td>821-00-0265</td>
      <td>Scientist</td>
      <td>19114.12</td>
    </tr>
  </tbody>
</table>
</div>




```python
CreditScoreTrain.columns
```




    Index(['ID', 'Customer_ID', 'Month', 'Name', 'Age', 'SSN', 'Occupation',
           'Annual_Income', 'Monthly_Inhand_Salary', 'Num_Bank_Accounts',
           'Num_Credit_Card', 'Interest_Rate', 'Num_of_Loan', 'Type_of_Loan',
           'Delay_from_due_date', 'Num_of_Delayed_Payment', 'Changed_Credit_Limit',
           'Num_Credit_Inquiries', 'Credit_Mix', 'Outstanding_Debt',
           'Credit_Utilization_Ratio', 'Credit_History_Age',
           'Payment_of_Min_Amount', 'Total_EMI_per_month',
           'Amount_invested_monthly', 'Payment_Behaviour', 'Monthly_Balance',
           'Credit_Score'],
          dtype='object')




```python
cur,conn = create_database()
```


```python
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS CreditScoreTest(
ID VARCHAR PRIMARY KEY, 
Customer_ID VARCHAR, 
Month VARCHAR, 
Name VARCHAR, 
Age VARCHAR, 
SSN VARCHAR, 
Occupation VARCHAR
)""")
```


```python
cur.execute(songplay_table_create)
conn.commit()
```


```python
songplaying_table_create = ("""CREATE TABLE IF NOT EXISTS CreditScoreTrain(
ID VARCHAR, 
Customer_ID VARCHAR, 
Month VARCHAR, 
Name VARCHAR, 
Age VARCHAR, 
SSN VARCHAR, 
Occupation VARCHAR,
Annual_Income VARCHAR
)""")
```


```python
cur.execute(songplaying_table_create)
conn.commit()
```


```python
songplay_table_insert = ("""INSERT INTO CreditScoreTest(
ID, 
Customer_ID, 
Month, 
Name, 
Age, 
SSN, 
Occupation)
VALUES(%s, %s, %s, %s, %s, %s, %s)
""")
```


```python
for i, row in CreditScoreTest_clean.iterrows():
        cur.execute(songplay_table_insert, list(row))
```


```python
conn.commit()
```


```python
songplaying_table_insert = ("""INSERT INTO CreditScoreTrain(
ID, 
Customer_ID, 
Month, 
Name, 
Age, 
SSN, 
Occupation,
Annual_Income)
VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
""")
```


```python
for i, row in CreditScoreTrain_clean.iterrows():
        cur.execute(songplaying_table_insert, list(row))
```


```python
conn.commit()
```


```python

```
