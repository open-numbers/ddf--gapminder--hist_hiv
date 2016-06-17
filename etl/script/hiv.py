# -*- coding: utf-8 -*-

import pandas as pd
import os
from ddf_utils.str import to_concept_id
from ddf_utils.index import create_index_file


# configuration of file paths
source_plwha = '../source/indicator_hiv_plwha - Data.csv'
source_prevalence = '../source/indicator hiv estimated prevalence% 15-49 - Data.csv'

out_dir = '../../'


def extract_datapoints(data, cname):
    data = data.set_index(data.columns[0])
    data = data.unstack()
    data = data.reset_index()
    data.columns = ['year', 'country', cname]
    return data.dropna().sort_values(by=['country', 'year'])


def extract_entities_country(data):
    country = data.iloc[:, [0]].copy()
    country.columns = ['name']

    country['country'] = country['name'].map(to_concept_id)

    return country.drop_duplicates()


if __name__ == '__main__':

    data_plwha = pd.read_csv(source_plwha)
    data_prevalence = pd.read_csv(source_prevalence)

    conc = [data_plwha.columns[0], data_prevalence.columns[0]]
    conc_id = list(map(to_concept_id, conc))

    cdf = pd.DataFrame([], columns=['concept', 'name', 'concept_type'])
    cdf['name'] = conc
    cdf['concept'] = conc_id
    cdf['concept_type'] = 'measure'

    cdf = cdf.append(pd.DataFrame([['name', 'Name', 'string'],
                                   ['year', 'Year', 'time'],
                                   ['country', 'Country', 'entity_domain']],
                                   columns=cdf.columns))

    path = os.path.join(out_dir, 'ddf--concepts.csv')
    cdf.to_csv(path, index=False)

    # entities
    ent = extract_entities_country(data_plwha)
    path = os.path.join(out_dir, 'ddf--entities--country.csv')
    ent.to_csv(path, index=False)

    # datapoint
    dps_plwha = extract_datapoints(data_plwha, conc_id[0])
    dps_prevalance = extract_datapoints(data_prevalence, conc_id[1])

    path1 = os.path.join(out_dir,
                         'ddf--datapoints--{}--by--country--year.csv'.format(conc_id[0]))
    dps_plwha.to_csv(path1, index=False)

    path2 = os.path.join(out_dir,
                         'ddf--datapoints--{}--by--country--year.csv'.format(conc_id[1]))
    dps_prevalance.to_csv(path2, index=False)

    # index
    create_index_file(out_dir)

    print('Done.')

