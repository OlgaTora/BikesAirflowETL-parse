import pandas as pd
from dags.logger import logger
from sklearn.impute import SimpleImputer


class Transform:

    def __init__(self, data):
        self.data = data
        self.data_describe = data.describe(include='all')

    def transform(self):
        self.data = self.transform_dates()
        self.data = self.transform_dates_timedelta()
        logger.info('Data features transform to data format')
        print(self.data.info())
        return self.data

    def transform_dates_timedelta(self):
        if 'DOB' in self.data.columns:
            most_freq = self.data_describe['DOB']['top']
            self.data['DOB'].fillna(most_freq, inplace=True)
            self.data['DOB'] = pd.to_datetime(self.data['DOB'])
        return self.data

    def transform_dates(self):
        if {'transaction_date', 'product_first_sold_date'}.issubset(self.data.columns):
            most_freq = SimpleImputer(strategy='most_frequent')
            self.data['product_first_sold_date'] = most_freq.fit_transform(self.data[['product_first_sold_date']])
            self.data['product_first_sold_date'] = self.data['product_first_sold_date'].astype(float)
            self.data['product_first_sold_date'] = (
                    pd.to_datetime("1900-01-01") + pd.to_timedelta(self.data['product_first_sold_date'], unit="D"))
            self.data['transaction_date'] = pd.to_datetime(self.data['transaction_date'])
        return self.data
