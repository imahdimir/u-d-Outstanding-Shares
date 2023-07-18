"""

    """

import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from githubdata import GitHubDataRepo as GDR
from mirutil.df import save_df_as_prq
from mirutil.async_req import get_resps_async_sync
from mirutil.utils import ret_clusters_indices
from persiantools.jdatetime import JalaliDateTime
from mirutil.jdate import make_jdate_col_fr_str_date_col_in_a_df
from namespace_mahdimir.tse import DOutstandingSharesCol
from mirutil.df import reorder_df_cols_as_a_class_values

from main import c
from main import cn
from main import fpn
from main import gdu

# namespace     %%%%%%%%%%%%%%%
cd = DOutstandingSharesCol()

class Const :
    # the url format
    url = 'http://cdn.tsetmc.com/api/Instrument/GetInstrumentHistory/{}/{}'

def make_the_url(firmticker_id , date) :
    k = Const()
    return k.url.format(firmticker_id , date)

def make_the_url_col(df) :
    fu = lambda ro : make_the_url(ro[c.tse_id] , ro[c.d].replace('-' , ''))
    df[cn.url] = df.apply(fu , axis = 1)
    return df

def add_resp_cols(df) :
    df[[cn.rst , cd.os]] = None
    return df

def read_outstanding_shares(res) :
    if res.cont is None :
        return None

    return res.cont['instrumentHistory']['zTitad']

def add_resps_to_df(df , inds , resps) :
    cols = {
            cn.rst : lambda x : x.r.status ,
            cd.os  : read_outstanding_shares ,
            }

    for ky , vl in cols.items() :
        df.loc[inds , ky] = [vl(x) for x in resps]

    msk = df.loc[inds , cn.rst].eq(200)
    print('new data count: ' , len(msk[msk]))

    return df

def enumerate_in_group(df) :
    df = df.sort_values(c.d , ascending = True)

    df[cn.one] = 1
    df[cn.n] = df.groupby(c.tse_id)[cn.one].cumsum()

    df = df.drop(columns = [cn.one])

    return df

def mark_spaced_ones(df , space_days: int) :
    df[cn.mkd] = df[cn.n].mod(space_days).eq(1)
    return df

def filter_spaced_ones(df) :
    msk = df[cn.rst].ne(200)
    msk &= df[cn.mkd]

    df = df[msk]
    print('spaced ones:' , len(df))

    return df

def filter_to_get_items(df) :
    msk = df[cn.rcnt].isna()
    msk |= df[cn.rst].ne(200)

    df = df[msk]
    print('empty ones count:' , len(df))

    return df

def get_all_data(df , filter_func , test_mode = True) :
    """

    """

    _df = filter_func(df)

    cls = ret_clusters_indices(_df , 100)

    for se in cls :
        si = se[0]
        ei = se[1]
        print(se)

        inds = _df.iloc[si : ei].index
        print(inds)

        urls = df.loc[inds , cn.url]

        rs = get_resps_async_sync(urls , mode = 'json')

        df = add_resps_to_df(df , inds , rs)

        if test_mode :
            break

        time.sleep(1)

    return df

def fill_in_between_os_nan_values(df) :
    _c = cd.os

    df[cn.bfil] = df.groupby(c.tse_id)[_c].bfill()
    df[cn.ffil] = df.groupby(c.tse_id)[_c].ffill()

    msk = df[_c].isna()

    msk &= df[cn.bfil].eq(df[cn.ffil])

    df.loc[msk , _c] = df[cn.bfil]

    return df

def get_all_data_with_retry(df , filter_func) :
    """ get all data in number of loops and not test mode """
    try :

        for i in range(10) :
            print('Loop numebr: ' , i)

            df = get_all_data(df , filter_func , test_mode = False)

    except KeyboardInterrupt :
        print('KeyboardInterrupt')

    finally :
        return df

def count_nan_os_values(df) :
    msk = df[cd.os].isna()
    print(len(msk[msk]))

def make_os_col_int(df) :
    df[cd.os] = df[cd.os].astype('Int64')
    return df

def drop_below_50k_shares(df) :
    msk = df[cd.os].gt(50 * 10 ** 3)
    msk |= df[cd.os].isna()

    df = df[msk]

    return df

def main() :
    pass

    ##

    # get all id - date pairs
    df = pd.read_parquet(fpn.t0)

    ##
    df = make_the_url_col(df)

    ##
    def m1() :
        pass

        ##
        save_df_as_prq(df , fpn.t1_0)

        ##
        df = pd.read_parquet(fpn.t1_0)

    ##
    df = add_resp_cols(df)

    ##
    df = enumerate_in_group(df)

    ##
    df = mark_spaced_ones(df , 100)

    ##
    df = get_all_data_with_retry(df , filter_spaced_ones)

    ##
    def man2() :
        pass

        ##
        save_df_as_prq(df , fpn.t1_1)

        ##
        df = pd.read_parquet(fpn.t1_1)

    ##
    count_nan_os_values(df)

    ##
    df = make_os_col_int(df)

    ##
    df = drop_below_50k_shares(df)

    ##
    df = fill_in_between_os_nan_values(df)

    ##
    count_nan_os_values(df)

    ##
    def m3() :
        pass

        ##
        save_df_as_prq(df , fpn.t1_2)

        ##
        df = pd.read_parquet(fpn.t1_2)

    ##

    ##
    # save temp data without index
    save_df_as_prq(df , fpn.t1_1)

    ##

##


if __name__ == "__main__" :
    main()
    print(f'{Path(__file__).name} Done!')

##


def test() :
    pass

    ##
    from mirutil.const import Const as MConst

    # a single id
    url = make_the_url(17617474823279712 , '20110622')
    print(url)
    mk = MConst()

    res = requests.get(url , headers = mk.headers)
    x = res.json()
    print(res.json())
    x

    ##

    # 100 rows
    urls = df.iloc[:100][cn.url]
    resps = get_resps_async_sync(urls , mode = 'json')

    resps[0].cont

    ##

    # test
    df = get_all_data(df , filter_spaced_ones)

    ##

    ##

    ##
    import time
    from numpy import vectorize

    # Start timer
    start_time = time.time()

    # Code to be timed
    df = make_jdate_col_from_str_date_col(df , c.d , c.jd)

    # End timer
    end_time = time.time()

    # Calculate elapsed time
    elapsed_time = end_time - start_time
    print("Elapsed time: " , elapsed_time)

    ##
    df = make_jdate_col_fr_str_date_col_in_a_df(df , c.d , c.jd)

    ##
    df = reorder_df_cols_as_a_class_values(df , DOutstandingSharesCol)

    ##
    df = df.dropna()

    ##
    df[cd.os] = df[cd.os].astype(int)

    ##
    msk = df[cd.os].eq(1)
    df = df[~ msk]

    ##
    df = df.sort_values(c.d)

    ##
    df1 = df.drop_duplicates(subset = [c.ftic , cd.os] , keep = 'first')

    ##
    df2 = df.groupby(c.ftic).nth(-1)

    ##
    df3 = pd.concat([df1 , df2])

    ##
    df4 = df3.drop_duplicates()

    ##

    ##
    save_df_as_prq(df4 , 'temp.prq')

    ##

    ##

    ##

    ##

    ##

    ##

    ##

    ##
