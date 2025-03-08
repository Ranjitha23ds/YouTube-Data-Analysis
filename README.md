# 📊 YouTube Data Harvesting and Warehousing using SQL and Streamlit  

## 📘 Introduction  
This project involves:  
- Building a **dashboard/UI** using **Streamlit**.  
- Retrieving **YouTube channel data** via the **YouTube API**.  
- Storing data in a **MySQL database (warehousing) 
- Running **SQL queries** to analyze data and visualize it in **Streamlit**.  

📂 **Domain**: Social Media  
🛠 **Skills**: Python, API Integration, SQL

---

## **📘 Overview**  

### 🌾 **Data Harvesting**  
- Fetch **channel information, video details, playlists, comments** using the **YouTube Data API**.  

### 📥 **Data Storage**  
- Store collected data in a **MySQL database**   

### 📊 **Data Analysis & Visualization**  
- Query data using **SQL**.  
- Create **visualizations** using **Streamlit** application.  

---

## **🛠 Technology and Tools**  
- **Python 3.12.2**  
- **MySQL**  
- **YouTube API**  
- **Streamlit**  
---

## **📚 Packages and Libraries**  
```python
# YouTube API Client
from googleapiclient.discovery import build

# MySQL Connection
import mysql.connector
from sqlalchemy import create_engine

# Data Handling
import pandas as pd

# Streamlit (for UI)
import streamlit as st
from streamlit_option_menu import option_menu

