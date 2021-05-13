import pandas as pd
from multiprocessing import Pool

import constants
import spacy
from pathlib import Path
def load_model():
    output_dir = Path('content')
    nlp_updated = spacy.load(output_dir)
    doc = nlp_updated("By when will it be available?\xa0\nPlease select 1,2 ,3 or 4 \n1. 3 hours \n 2. 6 hours \n3. 12 hours \n4. 24 hours \n5. Don't Know ")
    print("Entities", [(ent.text, ent.label_) for ent in doc.ents])
    return nlp_updated

def load_suppliers_csv(csv_file_path):
    df = pd.read_csv(csv_file_path)
    #df = df[df['StatusDesc'] == 'REPLIED']
    return df


def retrieve_messages(supplier):
    supplier.get_parsed_messages()


def multiprocess_supplier_messages(suppliers):
    if len(suppliers) <= constants.MIN_POOL_SIZE:
        for supplier in suppliers:
            retrieve_messages(supplier)

    with Pool(4) as pool:
        pool.map(retrieve_messages, suppliers)
    
    return True