import pandas as pd
import numpy as np
import time
import multiprocessing

in_df = pd.read_csv("si_for_imputation.csv", low_memory = False)

def impute(i):
    out = in_df.loc[i,]
    
    if any(out.isnull()):
        attribute_list = ['foundation_type', 'num_stories', 'occupancy_type_nsi', 'sqft_total', 'ffe']
        dist = np.sqrt(np.square(in_df['lat'] - out['lat']) + np.square(in_df['lon'] - out['lon']))
        
        for attribute in attribute_list:
            if pd.isnull(out[attribute]):
                temp = in_df.loc[:,('structure_id', attribute)]
                temp['dist'] = dist
                temp = temp.iloc[temp[temp[attribute].notnull()].index.tolist()]
                temp = temp.reset_index()
                temp.sort_values(by='dist', axis=0, inplace=True)
                if attribute in ['foundation_type', 'num_stories', 'occupancy_type_nsi']:
                    out[attribute] = temp.head(10)[attribute].mode().tolist()[0]
                else:
                    out[attribute] = temp.head(10)[attribute].mean()

    return out
def main():
    start = time.time()
    pool = multiprocessing.Pool(4)

    data = list(pool.map(impute, range(1200001, 1300000)))
    out_df = pd.DataFrame(data)
    out_df.to_csv('si_imputed_full_1200001.csv')
    end = time.time()
    print('Time Elapsed')
    print(round(end-start, 3))



if __name__ == '__main__':
    
    
    main()  
    

    