import pandas as pd

df = pd.read_csv('./data/input/ceramics_sample_group_descriptions_20200107.txt', sep='\t')

df.columns = [
    c.lower() for c in df.columns
]

df = df.set_index('system_id')

from sqlalchemy import create_engine

engine = create_engine('postgresql://humlab_admin:Vua9VagZ@seadserv.humlab.umu.se:5432/sead_staging')

df.to_sql(name="temp_sample_group_descriptions", schema='clearing_house', con=engine)

