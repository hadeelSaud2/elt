
import glob
import pandas as pd
from datetime import datetime


!wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/bank_market_cap_1.json
!wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/bank_market_cap_2.json
!wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Final%20Assignment/exchange_rates.csv

def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process)
    return dataframe

columns=['Name','Market Cap (US$ Billion)']

def extract():
    
    fname = "bank_market_cap_1.json"
    df = extract_from_json(fname)
    df2 = df[columns]
    return df2



df_exr = pd.read_csv("exchange_rates.csv", index_col=0)
exchange_rate = df_exr.loc["GBP", "Rates"]
print(df_exr.head())
print()
print("exchange_rate = ", exchange_rate)

def transform(dataFrame, ex_rate):
   
    usd2gbp = dataFrame.loc["USD", "Rates"] / ex_rate
    df_usd = extract()
    df_usd.loc[:, "Market Cap (GBP$ Billion)"] = round(df_usd.iloc[:, 1] / usd2gbp, 3)
    df_gbp = df_usd.drop("Market Cap (US$ Billion)", axis=1)
    return df_gbp, usd2gbp

def load(data_frame, filename):
    data_frame.to_csv(filename, index=False)

def log(msg):
   
    with open("log.txt", "a") as f:
        dt = datetime.today().strftime("%Y-%m-%d %H:%M-%S")
        f.write(str(dt) + "   " + str(msg) + "\n")
              
log("ETL Job Started")
log("Extract phase Started")


df0 = extract()
df0.head()

log("Extract phase Ended")

log("Transform phase Started")


df_gbp, usd2gbp = transform(df_exr, exchange_rate) 
df_gbp.head()

log("Transform phase Ended")
log("Load phase Started")
load(df_gbp, "bank_market_cap_gbp.csv")  
log("Load phase Ended")

