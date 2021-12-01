# **Extract Transform Load (ETL) Lab**


## Objectives



*   Read CSV and JSON file types.
*   Extract data from the above file types.
*   Transform data.
*   Save the transformed data in a ready-to-load format which data engineers can use to load into an RDBMS.


Import the required modules and functions



```python
import glob                         # this module helps in selecting files 
import pandas as pd                 # this module helps in processing CSV files
import xml.etree.ElementTree as ET  # this module helps in processing XML files.
from datetime import datetime
```

## Download Files



```python
!wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/source.zip
```

    --2021-11-16 00:40:38--  https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/source.zip
    Resolving cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud (cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud)... 169.63.118.104
    Connecting to cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud (cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud)|169.63.118.104|:443... connected.
    HTTP request sent, awaiting response... 200 OK
    Length: 2707 (2.6K) [application/zip]
    Saving to: ‘source.zip.1’
    
    source.zip.1        100%[===================>]   2.64K  --.-KB/s    in 0s      
    
    2021-11-16 00:40:38 (72.1 MB/s) - ‘source.zip.1’ saved [2707/2707]
    


## Unzip Files



```python
!unzip source.zip
```

    Archive:  source.zip
    replace source3.json? [y]es, [n]o, [A]ll, [N]one, [r]ename: ^C


## Set Paths



```python
tmpfile    = "temp.tmp"               # file used to store all extracted data
logfile    = "logfile.txt"            # all event logs will be stored in this file
targetfile = "transformed_data.csv"   # file where transformed data is stored
```

## Extract


### CSV Extract Function



```python
def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe
```

### JSON Extract Function



```python
def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process,lines=True)
    return dataframe
```

### XML Extract Function



```python
def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns=["name", "height", "weight"])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for person in root:
        name = person.find("name").text
        height = float(person.find("height").text)
        weight = float(person.find("weight").text)
        dataframe = dataframe.append({"name":name, "height":height, "weight":weight}, ignore_index=True)
    return dataframe
```

### Extract Function



```python
def extract():
    # create an empty data frame to hold extracted data
    extracted_data = pd.DataFrame(columns=['name','height','weight']) 
    
    #process all csv files
    for csvfile in glob.glob("*.csv"):
        extracted_data = extracted_data.append(extract_from_csv(csvfile), ignore_index=True)
        
    #process all json files
    for jsonfile in glob.glob("*.json"):
        extracted_data = extracted_data.append(extract_from_json(jsonfile), ignore_index=True)
    
    #process all xml files
    for xmlfile in glob.glob("*.xml"):
        extracted_data = extracted_data.append(extract_from_xml(xmlfile), ignore_index=True)
        
    return extracted_data
```

## Transform


The transform function does the following tasks.

1.  Convert height which is in inches to millimeter
2.  Convert weight which is in pounds to kilograms



```python
def transform(data):
        #Convert height which is in inches to millimeter
        #Convert the datatype of the column into float
        #data.height = data.height.astype(float)
        #Convert inches to meters and round off to two decimals(one inch is 0.0254 meters)
        data['height'] = round(data.height * 0.0254,2)
        
        #Convert weight which is in pounds to kilograms
        #Convert the datatype of the column into float
        #data.weight = data.weight.astype(float)
        #Convert pounds to kilograms and round off to two decimals(one pound is 0.45359237 kilograms)
        data['weight'] = round(data.weight * 0.45359237,2)
        return data
```

## Loading



```python
def load(targetfile,data_to_load):
    data_to_load.to_csv(targetfile)  
```

## Logging



```python
def log(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("logfile.txt","a") as f:
        f.write(timestamp + ',' + message + '\n')
```

## Running ETL Process



```python
log("ETL Job Started")
```


```python
log("Extract phase Started")
extracted_data = extract()
log("Extract phase Ended")
extracted_data
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
      <th>name</th>
      <th>height</th>
      <th>weight</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>alex</td>
      <td>65.78</td>
      <td>112.99</td>
    </tr>
    <tr>
      <th>1</th>
      <td>ajay</td>
      <td>71.52</td>
      <td>136.49</td>
    </tr>
    <tr>
      <th>2</th>
      <td>alice</td>
      <td>69.40</td>
      <td>153.03</td>
    </tr>
    <tr>
      <th>3</th>
      <td>ravi</td>
      <td>68.22</td>
      <td>142.34</td>
    </tr>
    <tr>
      <th>4</th>
      <td>joe</td>
      <td>67.79</td>
      <td>144.30</td>
    </tr>
    <tr>
      <th>5</th>
      <td>alex</td>
      <td>65.78</td>
      <td>112.99</td>
    </tr>
    <tr>
      <th>6</th>
      <td>ajay</td>
      <td>71.52</td>
      <td>136.49</td>
    </tr>
    <tr>
      <th>7</th>
      <td>alice</td>
      <td>69.40</td>
      <td>153.03</td>
    </tr>
    <tr>
      <th>8</th>
      <td>ravi</td>
      <td>68.22</td>
      <td>142.34</td>
    </tr>
    <tr>
      <th>9</th>
      <td>joe</td>
      <td>67.79</td>
      <td>144.30</td>
    </tr>
    <tr>
      <th>10</th>
      <td>alex</td>
      <td>65.78</td>
      <td>112.99</td>
    </tr>
    <tr>
      <th>11</th>
      <td>ajay</td>
      <td>71.52</td>
      <td>136.49</td>
    </tr>
    <tr>
      <th>12</th>
      <td>alice</td>
      <td>69.40</td>
      <td>153.03</td>
    </tr>
    <tr>
      <th>13</th>
      <td>ravi</td>
      <td>68.22</td>
      <td>142.34</td>
    </tr>
    <tr>
      <th>14</th>
      <td>joe</td>
      <td>67.79</td>
      <td>144.30</td>
    </tr>
    <tr>
      <th>15</th>
      <td>jack</td>
      <td>68.70</td>
      <td>123.30</td>
    </tr>
    <tr>
      <th>16</th>
      <td>tom</td>
      <td>69.80</td>
      <td>141.49</td>
    </tr>
    <tr>
      <th>17</th>
      <td>tracy</td>
      <td>70.01</td>
      <td>136.46</td>
    </tr>
    <tr>
      <th>18</th>
      <td>john</td>
      <td>67.90</td>
      <td>112.37</td>
    </tr>
    <tr>
      <th>19</th>
      <td>jack</td>
      <td>68.70</td>
      <td>123.30</td>
    </tr>
    <tr>
      <th>20</th>
      <td>tom</td>
      <td>69.80</td>
      <td>141.49</td>
    </tr>
    <tr>
      <th>21</th>
      <td>tracy</td>
      <td>70.01</td>
      <td>136.46</td>
    </tr>
    <tr>
      <th>22</th>
      <td>john</td>
      <td>67.90</td>
      <td>112.37</td>
    </tr>
    <tr>
      <th>23</th>
      <td>jack</td>
      <td>68.70</td>
      <td>123.30</td>
    </tr>
    <tr>
      <th>24</th>
      <td>tom</td>
      <td>69.80</td>
      <td>141.49</td>
    </tr>
    <tr>
      <th>25</th>
      <td>tracy</td>
      <td>70.01</td>
      <td>136.46</td>
    </tr>
    <tr>
      <th>26</th>
      <td>john</td>
      <td>67.90</td>
      <td>112.37</td>
    </tr>
    <tr>
      <th>27</th>
      <td>simon</td>
      <td>67.90</td>
      <td>112.37</td>
    </tr>
    <tr>
      <th>28</th>
      <td>jacob</td>
      <td>66.78</td>
      <td>120.67</td>
    </tr>
    <tr>
      <th>29</th>
      <td>cindy</td>
      <td>66.49</td>
      <td>127.45</td>
    </tr>
    <tr>
      <th>30</th>
      <td>ivan</td>
      <td>67.62</td>
      <td>114.14</td>
    </tr>
    <tr>
      <th>31</th>
      <td>simon</td>
      <td>67.90</td>
      <td>112.37</td>
    </tr>
    <tr>
      <th>32</th>
      <td>jacob</td>
      <td>66.78</td>
      <td>120.67</td>
    </tr>
    <tr>
      <th>33</th>
      <td>cindy</td>
      <td>66.49</td>
      <td>127.45</td>
    </tr>
    <tr>
      <th>34</th>
      <td>ivan</td>
      <td>67.62</td>
      <td>114.14</td>
    </tr>
    <tr>
      <th>35</th>
      <td>simon</td>
      <td>67.90</td>
      <td>112.37</td>
    </tr>
    <tr>
      <th>36</th>
      <td>jacob</td>
      <td>66.78</td>
      <td>120.67</td>
    </tr>
    <tr>
      <th>37</th>
      <td>cindy</td>
      <td>66.49</td>
      <td>127.45</td>
    </tr>
    <tr>
      <th>38</th>
      <td>ivan</td>
      <td>67.62</td>
      <td>114.14</td>
    </tr>
  </tbody>
</table>
</div>




```python
log("Transform phase Started")
transformed_data = transform(extracted_data)
log("Transform phase Ended")
transformed_data 
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
      <th>name</th>
      <th>height</th>
      <th>weight</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>alex</td>
      <td>1.67</td>
      <td>51.25</td>
    </tr>
    <tr>
      <th>1</th>
      <td>ajay</td>
      <td>1.82</td>
      <td>61.91</td>
    </tr>
    <tr>
      <th>2</th>
      <td>alice</td>
      <td>1.76</td>
      <td>69.41</td>
    </tr>
    <tr>
      <th>3</th>
      <td>ravi</td>
      <td>1.73</td>
      <td>64.56</td>
    </tr>
    <tr>
      <th>4</th>
      <td>joe</td>
      <td>1.72</td>
      <td>65.45</td>
    </tr>
    <tr>
      <th>5</th>
      <td>alex</td>
      <td>1.67</td>
      <td>51.25</td>
    </tr>
    <tr>
      <th>6</th>
      <td>ajay</td>
      <td>1.82</td>
      <td>61.91</td>
    </tr>
    <tr>
      <th>7</th>
      <td>alice</td>
      <td>1.76</td>
      <td>69.41</td>
    </tr>
    <tr>
      <th>8</th>
      <td>ravi</td>
      <td>1.73</td>
      <td>64.56</td>
    </tr>
    <tr>
      <th>9</th>
      <td>joe</td>
      <td>1.72</td>
      <td>65.45</td>
    </tr>
    <tr>
      <th>10</th>
      <td>alex</td>
      <td>1.67</td>
      <td>51.25</td>
    </tr>
    <tr>
      <th>11</th>
      <td>ajay</td>
      <td>1.82</td>
      <td>61.91</td>
    </tr>
    <tr>
      <th>12</th>
      <td>alice</td>
      <td>1.76</td>
      <td>69.41</td>
    </tr>
    <tr>
      <th>13</th>
      <td>ravi</td>
      <td>1.73</td>
      <td>64.56</td>
    </tr>
    <tr>
      <th>14</th>
      <td>joe</td>
      <td>1.72</td>
      <td>65.45</td>
    </tr>
    <tr>
      <th>15</th>
      <td>jack</td>
      <td>1.74</td>
      <td>55.93</td>
    </tr>
    <tr>
      <th>16</th>
      <td>tom</td>
      <td>1.77</td>
      <td>64.18</td>
    </tr>
    <tr>
      <th>17</th>
      <td>tracy</td>
      <td>1.78</td>
      <td>61.90</td>
    </tr>
    <tr>
      <th>18</th>
      <td>john</td>
      <td>1.72</td>
      <td>50.97</td>
    </tr>
    <tr>
      <th>19</th>
      <td>jack</td>
      <td>1.74</td>
      <td>55.93</td>
    </tr>
    <tr>
      <th>20</th>
      <td>tom</td>
      <td>1.77</td>
      <td>64.18</td>
    </tr>
    <tr>
      <th>21</th>
      <td>tracy</td>
      <td>1.78</td>
      <td>61.90</td>
    </tr>
    <tr>
      <th>22</th>
      <td>john</td>
      <td>1.72</td>
      <td>50.97</td>
    </tr>
    <tr>
      <th>23</th>
      <td>jack</td>
      <td>1.74</td>
      <td>55.93</td>
    </tr>
    <tr>
      <th>24</th>
      <td>tom</td>
      <td>1.77</td>
      <td>64.18</td>
    </tr>
    <tr>
      <th>25</th>
      <td>tracy</td>
      <td>1.78</td>
      <td>61.90</td>
    </tr>
    <tr>
      <th>26</th>
      <td>john</td>
      <td>1.72</td>
      <td>50.97</td>
    </tr>
    <tr>
      <th>27</th>
      <td>simon</td>
      <td>1.72</td>
      <td>50.97</td>
    </tr>
    <tr>
      <th>28</th>
      <td>jacob</td>
      <td>1.70</td>
      <td>54.73</td>
    </tr>
    <tr>
      <th>29</th>
      <td>cindy</td>
      <td>1.69</td>
      <td>57.81</td>
    </tr>
    <tr>
      <th>30</th>
      <td>ivan</td>
      <td>1.72</td>
      <td>51.77</td>
    </tr>
    <tr>
      <th>31</th>
      <td>simon</td>
      <td>1.72</td>
      <td>50.97</td>
    </tr>
    <tr>
      <th>32</th>
      <td>jacob</td>
      <td>1.70</td>
      <td>54.73</td>
    </tr>
    <tr>
      <th>33</th>
      <td>cindy</td>
      <td>1.69</td>
      <td>57.81</td>
    </tr>
    <tr>
      <th>34</th>
      <td>ivan</td>
      <td>1.72</td>
      <td>51.77</td>
    </tr>
    <tr>
      <th>35</th>
      <td>simon</td>
      <td>1.72</td>
      <td>50.97</td>
    </tr>
    <tr>
      <th>36</th>
      <td>jacob</td>
      <td>1.70</td>
      <td>54.73</td>
    </tr>
    <tr>
      <th>37</th>
      <td>cindy</td>
      <td>1.69</td>
      <td>57.81</td>
    </tr>
    <tr>
      <th>38</th>
      <td>ivan</td>
      <td>1.72</td>
      <td>51.77</td>
    </tr>
  </tbody>
</table>
</div>




```python
log("Load phase Started")
load(targetfile,transformed_data)
log("Load phase Ended")
```


```python
log("ETL Job Ended")
```
