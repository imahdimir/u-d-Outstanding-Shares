"""

    """

import time
from pathlib import Path

import pandas as pd
import requests
from mirutil.async_req import get_resps_async_sync
from mirutil.df import reorder_df_cols_as_a_class_values
from mirutil.df import save_df_as_prq
from mirutil.jdate import make_jdate_col_fr_str_date_col_in_a_df
from mirutil.utils import ret_clusters_indices
from namespace_mahdimir.tse import D0OutstandingSharesCol

from main import c
from main import cd
from main import cn
from main import fpn

class Const :
    # the url format
    url = 'http://cdn.tsetmc.com/api/Instrument/GetInstrumentHistory/{}/{}'

def make_the_url(firmticker_id , date) :
    k = Const()
    return k.url.format(firmticker_id , date)

def make_the_url_col(df) :
    fu = lambda ro : make_the_url(ro[c.tse_id] , ro[c.d].replace('-' , ''))

    msk = df[c.tse_id].notna()

    df.loc[msk , cn.url] = df.apply(fu , axis = 1)

    return df

def add_resp_cols(df) :
    df[[cn.rst]] = None
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
    df[cn.isna] = df[cd.os].isna()

    df[cn.n] = df.groupby([c.tse_id , cn.isna])[cn.one].cumsum()

    df = df.drop(columns = [cn.one , cn.isna])

    return df

def mark_spaced_ones(df , space_days: int) :
    df[cn.mkd] = df[cn.n].mod(space_days).eq(1)
    return df

def filter_spaced_ones(df) :
    msk = df[cd.os].isna()
    msk &= df[cn.url].notna()

    msk &= df[cn.mkd]

    df = df[msk]
    print('spaced ones:' , len(df))

    return df

def filter_to_get_items(df) :
    msk = df[cd.os].isna()
    msk &= df[cn.url].notna()

    df = df[msk]
    print('empty ones count:' , len(df))

    return df

def get_all_data(df , filter_func , test_mode = True , chunk = 100) :
    """

    """

    _df = filter_func(df)

    cls = ret_clusters_indices(_df , chunk)

    for se in cls :
        si = se[0]
        ei = se[1]
        print(se)

        inds = _df.iloc[si : ei].index
        print(inds)

        urls = df.loc[inds , cn.url]

        rs = get_resps_async_sync(urls , mode = 'json' , timeout = 10)

        df = add_resps_to_df(df , inds , rs)

        if test_mode :
            break

        time.sleep(2)

    return df

def get_all_data_with_retry(df , filter_func , chunk , retry_count = 10) :
    """ get all data in number of loops and not test mode """

    try :

        for i in range(retry_count) :
            print('Loop numebr: ' , i)

            _fu = get_all_data
            df = _fu(df , filter_func , test_mode = False , chunk = chunk)

    except KeyboardInterrupt :
        print('KeyboardInterrupt')

    finally :
        return df

def fill_in_between_os_nan_values(df) :
    _c = cd.os

    df = df.sort_values(c.d)

    df[cn.bfil] = df.groupby(c.tse_id)[_c].bfill()
    df[cn.ffil] = df.groupby(c.tse_id)[_c].ffill()

    msk = df[_c].isna()

    msk &= df[cn.bfil].eq(df[cn.ffil])

    df.loc[msk , _c] = df[cn.bfil]

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

    df = pd.read_parquet(fpn.t1)

    ##
    df = make_the_url_col(df)

    ##
    def m1() :
        pass

        ##
        save_df_as_prq(df , fpn.t2_0)

        ##
        df = pd.read_parquet(fpn.t2_0)

    ##
    df = add_resp_cols(df)

    ##
    count_nan_os_values(df)

    ##
    df = enumerate_in_group(df)

    ##
    df = mark_spaced_ones(df , 60)

    ##
    _fu = get_all_data_with_retry
    df = _fu(df , filter_spaced_ones , 100 , retry_count = 1)

    ##
    def man2() :
        pass

        ##
        save_df_as_prq(df , fpn.t2_1)

        ##
        df = pd.read_parquet(fpn.t2_1)

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
        save_df_as_prq(df , fpn.t2_2)

        ##
        df = pd.read_parquet(fpn.t2_2)

    ##
    count_nan_os_values(df)

    ##
    df = df.sort_values(c.d , ascending = False)

    ##
    _fu = get_all_data_with_retry
    df = _fu(df , filter_to_get_items , 100)

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
    df = make_jdate_col_fr_str_date_col_in_a_df(df , c.d , c.jd)

    ##
    df = reorder_df_cols_as_a_class_values(df , D0OutstandingSharesCol)

    ##
    df = df.dropna()

    ##

    # save temp data without index
    save_df_as_prq(df , fpn.t2_3)

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
