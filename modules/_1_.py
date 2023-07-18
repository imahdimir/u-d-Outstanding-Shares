"""

    """

import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from githubdata import GitHubDataRepo as GDR
from mirutil.const import Const as MConst
from mirutil.df import save_df_as_prq
from mirutil.async_req import get_resps_async_sync
from mirutil.utils import ret_clusters_indices
from persiantools.jdatetime import JalaliDateTime
from mirutil.jdate import make_jdate_col_fr_str_date_col_in_a_df

from main import c
from main import cn
from main import fpn
from main import gdu

mk = MConst()

class Const :
    # nominal prices url format
    url = 'http://cdn.tsetmc.com/api/Instrument/GetInstrumentHistory/{}/{}'

def make_the_url(firmticker_id , date) :
    k = Const()
    return k.url.format(firmticker_id , date)

def make_the_url_col(df) :
    fu = lambda ro : make_the_url(ro[c.tse_id] , ro[c.d].replace('-' , ''))
    df[cn.url] = df.apply(fu , axis = 1)
    return df

def add_resp_cols(df) :
    df[[cn.rst , 'OS']] = None
    return df

def read_outstanding_shares(res) :
    if res.cont is None :
        return None

    return res.cont['instrumentHistory']['zTitad']

def add_resps_to_df(df , inds , resps) :
    cols = {
            cn.rst : lambda x : x.r.status ,
            'OS'   : read_outstanding_shares ,
            }

    for ky , vl in cols.items() :
        df.loc[inds , ky] = [vl(x) for x in resps]

    msk = df.loc[inds , cn.rst].eq(200)
    print('new data count: ' , len(msk[msk]))

    return df

def number_in_group(df) :
    df = df.sort_values(c.d , ascending = True)
    df['1'] = 1
    df['n'] = df.groupby(c.tse_id)['1'].cumsum()
    return df

def mark_spaced_ones(df , space_days: int) :
    df['n1'] = df['n'].mod(space_days).eq(1)
    return df

def filter_spaced_ones(df) :
    msk = df[cn.rst].ne(200)
    msk &= df['n1']

    df = df[msk]
    print('spaced ones:' , len(df))

    return df

def filter_to_get_items(df) :
    msk = df[cn.rcnt].isna()
    msk |= df[cn.rst].ne(200)

    df = df[msk]
    print('empty ones count:' , len(df))

    return df

def get_all_data(df , filter_func , test = True) :
    """

    """

    try :
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

            if test :
                break

            time.sleep(.5)

        return df

    except KeyboardInterrupt :
        return df

def fill_in_between_os(df) :
    df['bfill'] = df.groupby(c.tse_id)['OS'].bfill()
    df['ffill'] = df.groupby(c.tse_id)['OS'].ffill()

    msk = df['OS'].isna()

    msk &= df['bfill'].eq(df['ffill'])

    df.loc[msk , 'OS'] = df['bfill']

    return df

def main() :
    pass

    ##

    # get all id and dates
    df = pd.read_parquet(fpn.t0)

    ##
    df = make_the_url_col(df)

    ##
    save_df_as_prq(df , fpn.t1_0)

    ##
    df = pd.read_parquet(fpn.t1_0)

    ##
    df = add_resp_cols(df)

    ##
    df = number_in_group(df)

    ##
    df = mark_spaced_ones(df , 45)

    ##
    df = get_all_data_with_retry(df , filter_spaced_ones)

    ##
    df.to_parquet('t1' , index = False)

    ##
    df = fill_in_between_os(df)

    ##

    ##

    ##

    ##
    msk = df['OS'].isna()

    print(len(msk[msk]))

    ##

    ##

    ##
    df = pd.read_parquet(fpn.t1_1)

    ##
    df = get_all_data(df , test = False)

    ##
    # save temp data without index
    save_df_as_prq(df , fpn.t1_1)

    ##

##


if __name__ == "__main__" :
    main()
    print(f'{Path(__file__).name} Done!')

##


if False :
    pass

    ##
    def testf() :
        pass

        ##

        # a single id
        url = make_the_url(17617474823279712 , '20110622')
        print(url)
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
        x = df.iloc[0][cn.resp]

        ##

        # test
        df = get_all_data(df)

        ##
        fp = '/Users/mahdi/tept Dropbox/Mahdi Mir/GitHub/u-d-Outstanding-Shares/OutstandingShares-Daily.prq'
        df1 = pd.read_parquet(fp)

        df1 = df1.rename(columns = {
                'Ticker' : c.ftic
                })

        ##
        df = pd.read_parquet('t1')

        ##
        df['c'] = df.groupby([c.ftic , c.d])[c.tse_id].transform('count')

        ##
        df = make_jdate_col_from_str_date_col(df , c.d , c.jd)

        ##
        df = df.merge(df1 , on = [c.ftic , c.jd] , how = 'left')

        ##
        msk = df['c'].eq(1)
        msk &= df['OS'].isna()

        print(len(msk[msk]))

        ##
        df.loc[msk , 'OS'] = df['OutstandingShares']

        ##
        df['OS'] = df['OS'].astype(float)
        df['OS'] = df['OS'].astype('Int64')
        df['OS'] = df['OS'].astype('string')

        ##
        o1 = 'OutstandingShares'
        df[o1] = df[o1].astype(float)
        df[o1] = df[o1].astype('Int64')
        df[o1] = df[o1].astype('string')

        ##
        msk = df['OS'].ne(df['OutstandingShares'])
        msk &= df['OutstandingShares'].notna()
        msk &= df['OS'].notna()

        df2 = df[msk]

        ##
        df2.to_parquet('manual.prq' , index = False)

        ##
        df.loc[msk , 'OS'] = None

        ##
        df.to_parquet('t1' , index = False)

        ##

        ##
        msk = df['OS'].isna()
        print(len(msk[msk]))

        ##
        df = fill_in_between_os(df)

        ##
        msk = df['OS'].isna()
        print(len(msk[msk]))

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
        d = c.d
        jd = c.jd

        ##

        ##

        ##

        ##

        ##

        ##
