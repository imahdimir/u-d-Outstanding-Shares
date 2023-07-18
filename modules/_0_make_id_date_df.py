"""

    """

from datetime import datetime
from pathlib import Path

import pandas as pd
from githubdata import GitHubDataRepo as GDR
from mirutil.const import Const as MConst
from mirutil.df import save_df_as_prq

from main import c
from main import fpn
from main import gdu

mk = MConst()

def get_all_firm_ids() :
    """ get all Firms' TSETMC ids """
    gdr = GDR(gdu.firm_ids_s)
    df = gdr.read_data()
    df = df.astype('string')
    return df

def get_all_nominal_prices() :
    """ get all nominal prices """
    gdr = GDR(gdu.nom_price_s)
    df = gdr.read_data()
    df = df.astype('string')
    return df

def make_date_range_df(df_prices) :
    """ make a df with all dates from min date of price data to today """

    min_date = df_prices[c.d].min()
    tod_date = datetime.today().strftime('%Y-%m-%d')

    x = pd.date_range(min_date , tod_date , freq = 'D')

    dfd = pd.DataFrame(x , columns = [c.d])

    return dfd

def make_all_id_dates_by_cartesian_product(df_id , df_date) :
    return df_id.merge(df_date , how = 'cross')

def add_min_and_max_dates(df , df_prices) :
    """ add min and max dates for each tsetmc id """

    _df = df_prices.groupby(c.tse_id)[c.d].aggregate(['min' , 'max'])
    _df = _df.reset_index()

    df = df.merge(_df , on = c.tse_id , how = 'left')

    return df

def remove_rows_with_dates_out_of_range(df , df_prices) :
    """
    remove rows with dates that their TESETMC ids have no data for them
          in nominal prices data, or maybe their id have changed.
    """
    max_date = df_prices[c.d].max()

    # rows with dates less than min date in data
    msk1 = df[c.d] < df['min']

    # rows with dates greater than max date in data, when their max is
    #     less than data update date
    msk2 = df[c.d] > df['max']
    msk2 &= df['max'] < max_date

    msk = msk1 | msk2

    df = df[~ msk]

    # remove temp min and max cols
    df = df.drop(columns = ['min' , 'max'])

    return df

def main() :
    pass

    ##

    # get all firm ids
    dfi = get_all_firm_ids()

    ##

    # get all nominal prices
    dfp = get_all_nominal_prices()

    ##
    dfd = make_date_range_df(dfp)

    ##
    df = make_all_id_dates_by_cartesian_product(dfi , dfd)

    ##
    df = add_min_and_max_dates(df , dfp)

    ##
    df = remove_rows_with_dates_out_of_range(df , dfp)

    ##
    df[c.d] = df[c.d].dt.strftime('%Y-%m-%d')

    ##
    save_df_as_prq(df , fpn.t0)

    ##

##


if __name__ == "__main__" :
    main()
    print(f'{Path(__file__).name} Done!')

##


def test() :
    pass

    ##

    ##

    ##
