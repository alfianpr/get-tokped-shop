import pandas as pd
import datetime

def jobtime() -> datetime:
  sa_time = datetime.datetime.now()
  jobtime = datetime.datetime.strptime(sa_time.strftime('%Y-%m-%d %H:%M:%S'),'%Y-%m-%d %H:%M:%S')
  return jobtime

def standarized_columns(df: pd.DataFrame) -> pd.DataFrame:
    replace = {' ' : '_',
               '-' : '_',
               '.' : '_',
               ' ' : '_',
               '__' : ''}
    
    df.columns = df.columns.str.lower()
    for i, j in replace.items():
        df.columns = df.columns.str.replace(i, j, regex=False)
    df["scrape_time"] = jobtime()
    return df

def save_df_to_csv(df, dir, file_name, header):
  df.to_csv(f"{dir}/{file_name.lower().replace(' ', '_')}.csv", 
            index=False, 
            mode='a',
            header=header)