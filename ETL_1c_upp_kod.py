#!/usr/bin/env python
# coding: utf-8

# pip install pymssql 
# 
# 

# In[1]:


import pandas as pd
import pymssql 


# In[2]:


server="hw-dev-sql-02"
base="DEV_upp_nm82_2011_powerbi"


# In[3]:


sql="""
SELECT CAST(_Active AS int) AS Активность
, _LineNo AS НомерСтроки
, CAST(_RecorderRRef AS uniqueidentifier) AS Регистратор
, DATEADD([YEAR], -2000, _Period) AS Период
, CAST(_Fld22958_RRRef AS uniqueidentifier) AS БанковскийСчетКасса
, CAST(_Fld22959RRef AS uniqueidentifier) AS ВидДенежныхСредств
, CAST(_Fld22960RRef AS uniqueidentifier) AS ПриходРасход
, CAST(_Fld22961RRef AS uniqueidentifier) AS СтатьяДвиженияДенежныхСредств
, CAST(_Fld22962_RRRef AS uniqueidentifier) AS ДокументДвижения
, CAST(_Fld22963_RRRef AS uniqueidentifier) AS Контрагент
, CAST(_Fld22964RRef AS uniqueidentifier) AS ДоговорКонтрагента
, CAST(_Fld22965_RRRef AS uniqueidentifier) AS Сделка
, CAST(_Fld22966RRef AS uniqueidentifier) AS Проект
, CAST(_Fld22967_RRRef AS uniqueidentifier) AS ДокументПланированияПлатежа
, CAST(_Fld22968_RRRef AS uniqueidentifier) AS ДокументРасчетовСКонтрагентом
, CAST(_Fld22969RRef AS uniqueidentifier) AS Организация
, CAST(_Fld22970RRef AS uniqueidentifier) AS ЦФО
, CAST(_Fld22971RRef AS uniqueidentifier) AS СтатьяОборотовПоБюджетам
, _Fld22972 AS Сумма
, _Fld22973 AS СуммаУпр
FROM _AccumRg22957

"""


# In[5]:


def read_sql(sql,base, serv):
    with pymssql.connect(server=serv ,database = base ,charset = 'cp1251') as  conn:
                      
    
        cursor = conn.cursor()  
        df = pd.read_sql( sql,conn)
    return df


# conn.close()

# In[8]:


df=read_sql(sql,base, server)


# In[9]:


df


# In[ ]:




