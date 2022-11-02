import pandas as pd 
import sqlalchemy as sqlal

data_property = pd.read_csv('dataset/TR_PropertyInfo.csv')
data_user = pd.read_csv('dataset/TR_UserInfo.csv')

col_p = {"Prop ID": "prop_id", "PropertyCity": "property_city",  "PropertyState": "property_state"}
col_u = {"UserID": "user_id", "UserSex": "user_sex", "UserDevice": "user_device"}

data_property = data_property.rename(columns=col_p)
data_user = data_user.rename(columns=col_p)

conn = sqlal.create_engine(url='postgresql://postgres:sword1st@localhost:5432/project_4')
# conn = psy.connect(database="project_4", user="postgres", password="sword1st", host="localhost", port="5432")

data_property.to_sql('propery',con= conn, index=False, if_exists='replace')
data_user.to_sql('user',con= conn, index=False, if_exists='replace')
