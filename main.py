
import pandas as pd
import psycopg2
from newsapi import NewsApiClient
import os
newsapi = NewsApiClient(api_key='30449eaef55c4cf195a595e35920efc0')
# /v2/top-headlines
headlines = newsapi.get_top_headlines(q='AI',
                                        category='health',
                                          language='en',
                                          country='us')

headlines['articles']
df = pd.json_normalize(headlines['articles'])
df = df.rename(columns={'publishedAt': 'published_date', 'source.name':'source_name'})
df.drop(columns=['urlToImage','source.id'], inplace = True)

db_params = {
    "dbname": "postgres",
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": "5432"
}


try:
    # Connect to PostgreSQL
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    
    # SQL Insert Query
    insert_query = """
    INSERT INTO health_news (author, title, description, url, published_date, content,source_name)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (url) DO NOTHING;  -- Avoids duplicate primary key errors
    """
    
    # Insert DataFrame records one by one
    for _, row in df.iterrows():
        cursor.execute(insert_query, (
            row['author'], row['title'], row['description'],  row['url'], row['published_date'], row['content'],
            row['source_name']
        ))

    # Commit and close
    conn.commit()
    print("Data inserted successfully!")

except Exception as e:
    print("Error:", e)

finally:
    if conn:
        cursor.close()
        conn.close()
