import pandas as pd
from multiprocessing import Pool

import constants


def load_suppliers_csv(csv_file_path):
    df = pd.read_csv(csv_file_path)
    return df


def retrieve_messages(supplier):
    supplier.get_parsed_messages()


def multiprocess_supplier_messages(suppliers):
    if len(suppliers) <= constants.MIN_POOL_SIZE:
        for supplier in suppliers:
            retrieve_messages(supplier)

    with Pool(4) as pool:
        pool.map(retrieve_messages, suppliers)
