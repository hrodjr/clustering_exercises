import pandas as pd
import env

#db access
def get_connection(db, user=env.user, host=env.host, password=env.password):
    return f'mysql+pymysql://{user}:{password}@{host}/{db}'

#1. Includes all the tables in the zillow database.
#2. Used LEFT JOIN to keep all columns of the properties_2017 table
#3. Used a subquerry to filter DISTINCT property id with 2017 transaction
#4. WHERE statement to keep latitude/longitude where IS NOT NULL.
#zillow db
zillow_sql = "SELECT *\
                FROM properties_2017\
                LEFT JOIN predictions_2017 USING(parcelid)\
                LEFT JOIN airconditioningtype USING(airconditioningtypeid)\
                LEFT JOIN architecturalstyletype USING(architecturalstyletypeid)\
                LEFT JOIN buildingclasstype USING(buildingclasstypeid)\
                LEFT JOIN heatingorsystemtype USING(heatingorsystemtypeid)\
                LEFT JOIN propertylandusetype USING(propertylandusetypeid)\
                LEFT JOIN storytype USING(storytypeid)\
                LEFT JOIN typeconstructiontype USING(typeconstructiontypeid)\
                LEFT JOIN unique_properties USING(parcelid)\
                WHERE properties_2017.id IN(\
                SELECT DISTINCT id\
                FROM properties_2017\
                WHERE predictions_2017.transactiondate LIKE '2017%%') AND latitude IS NOT NULL;"

#acquires zillow dataset
def get_zillow_data():
    return pd.read_sql(zillow_sql,get_connection('zillow'))