#!/usr/bin/env python
# coding: utf-8

# In[3]:


import pandas as pd  
file = pd.read_csv(‘exchange_rates.csv’)  


# In[4]:


file.head()


# In[6]:


def transform(file):
       
        file['height'] = round(file.height * 0.0254,2)
        
       
        file['weight'] = round(file.weight * 0.45359237,2)
        return file

