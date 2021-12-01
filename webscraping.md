## Data Engineer - Webscraping

The two easiest and legal methods to collect data from other webiste is to through `URL` and `APIs`.

In this part you will:

*   Use webscraping to get bank information

For this lab, we are going to be using Python and several Python libraries. 

* Collect data using APIs

* Collect data using webscraping.

* Download files to process.    

* Read csv, xml and json file types.

* Extract data from the above file types.

* Transform data.

* Use the built in logging module.

* Save the transformed data in a ready-to-load format which data engineers can use to load the data.


```python
#!pip install pandas
#!pip install bs4
#!pip install requests
```


```python
from bs4 import BeautifulSoup
import requests
import pandas as pd
```

# 1. Extract Data Using Web Scraping

The wikipedia webpage [https://en.wikipedia.org/wiki/List_of_largest_banks](https://en.wikipedia.org/wiki/List_of_largest_banks?utm_medium=Exinfluencer&utm_source=Exinfluencer&utm_content=000026UJ&utm_term=10006555&utm_id=NA-SkillsNetwork-Channel-SkillsNetworkCoursesIBMDeveloperSkillsNetworkPY0221ENSkillsNetwork23455645-2021-01-01) provides information about largest banks in the world by various parameters. Scrape the data from the table 'By market capitalization' and store it in a JSON file.


```python
url = "https://en.wikipedia.org/wiki/List_of_largest_banks"
html_data  = requests.get(url).text
```


```python
html_data[101: 124]
```




    'List of largest banks -'




```python
soup = BeautifulSoup(html_data,"html5lib")
```


```python
data = pd.DataFrame(columns=['Name','Market Cap(US$ Billion)'])

for row in soup.find_all("tbody")[3].find_all("tr"):
    col = row.find_all("td")
    
    if (col != []):
        marketcap = col[-1].text.strip()
        name = col[1].text.strip()
        data = data.append({"Name":name, "Market Cap(US$ Billion)":marketcap}, ignore_index=True)
```


```python
data
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
      <th>Name</th>
      <th>Market Cap(US$ Billion)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>JPMorgan Chase</td>
      <td>488.470</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Bank of America</td>
      <td>379.250</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Industrial and Commercial Bank of China</td>
      <td>246.500</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Wells Fargo</td>
      <td>308.013</td>
    </tr>
    <tr>
      <th>4</th>
      <td>China Construction Bank</td>
      <td>257.399</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>65</th>
      <td>Ping An Bank</td>
      <td>37.993</td>
    </tr>
    <tr>
      <th>66</th>
      <td>Standard Chartered</td>
      <td>37.319</td>
    </tr>
    <tr>
      <th>67</th>
      <td>United Overseas Bank</td>
      <td>35.128</td>
    </tr>
    <tr>
      <th>68</th>
      <td>QNB Group</td>
      <td>33.560</td>
    </tr>
    <tr>
      <th>69</th>
      <td>Bank Rakyat Indonesia</td>
      <td>33.081</td>
    </tr>
  </tbody>
</table>
<p>70 rows × 2 columns</p>
</div>




```python
data.to_json("bank_market_cap.json")
```


```python

```

# 2. Extract Data Using an API

Using ExchangeRate-API we will extract currency exchange rate data. Use the below steps to get the access key and to get the data.

1.  Open the url : [https://exchangeratesapi.io/](https://exchangeratesapi.io/?utm_medium=Exinfluencer&utm_source=Exinfluencer&utm_content=000026UJ&utm_term=10006555&utm_id=NA-SkillsNetwork-Channel-SkillsNetworkCoursesIBMDeveloperSkillsNetworkPY0221ENSkillsNetwork23455645-2021-01-01) and create a free account.
2.  Once the account is created. You will get the Get the Free API key option on the top as shown below:

<img src="https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Final%20Assignment/Images/getapi.png"/>

3.  Copy the API key and use in the url in Question 1.



```python
url='http://api.exchangeratesapi.io/v1/latest?access_key=xxxxxxxxxxxxxxx'
response = requests.get(url)
# turn the data to json format
data = response.json()
```


```python
# Transform data to dataframe in python
df = pd.DataFrame(data, columns=['rates'])
```


```python
df
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
      <th>rates</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>AED</th>
      <td>4.179940</td>
    </tr>
    <tr>
      <th>AFN</th>
      <td>104.068702</td>
    </tr>
    <tr>
      <th>ALL</th>
      <td>121.593690</td>
    </tr>
    <tr>
      <th>AMD</th>
      <td>541.402700</td>
    </tr>
    <tr>
      <th>ANG</th>
      <td>2.051560</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
    </tr>
    <tr>
      <th>YER</th>
      <td>284.781010</td>
    </tr>
    <tr>
      <th>ZAR</th>
      <td>17.289996</td>
    </tr>
    <tr>
      <th>ZMK</th>
      <td>10243.244895</td>
    </tr>
    <tr>
      <th>ZMW</th>
      <td>19.912963</td>
    </tr>
    <tr>
      <th>ZWL</th>
      <td>366.431208</td>
    </tr>
  </tbody>
</table>
<p>168 rows × 1 columns</p>
</div>




```python
# Save the Dataframe
df.to_csv("exchange_rates_1.csv")
```


```python

```
