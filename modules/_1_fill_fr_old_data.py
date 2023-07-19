"""

    """

from pathlib import Path

import pandas as pd
from githubdata import GitHubDataRepo
from mirutil.df import save_df_as_prq

from main import c
from main import cd
from main import count_nan_os_values
from main import fpn
from main import gdu

def get_0_data() :
    gdt = GitHubDataRepo(gdu.os0_st)
    df = gdt.read_data()

    df = df[[c.tse_id , c.d , cd.os]]
    return df

def remove_before_a_date_manually(df) :
    """ By running the code like 1k time I realized that before this date
        the data cannot be retrieved at leaset by this method/channed(url).
    """
    d = '2011-06-26'

    msk = df[c.d].gt(d)
    df = df[msk]

    return df

def main() :
    pass

    ##
    df = pd.read_parquet(fpn.t0)

    ##
    dfo = get_0_data()

    ##

    # merge old data
    df = df.merge(dfo , on = [c.tse_id , c.d] , how = 'left')

    ##
    count_nan_os_values(df)

    ##
    df = remove_before_a_date_manually(df)

    ##
    count_nan_os_values(df)

    ##

    # save temp data
    save_df_as_prq(df , fpn.t1)

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
