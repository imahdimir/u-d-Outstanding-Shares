"""

    """

from pathlib import Path

import pandas as pd
from mirutil.df import reorder_df_cols_as_a_class_values
from mirutil.df import save_df_as_prq
from namespace_mahdimir.tse import DOutstandingSharesCol
from persiantools.jdatetime import JalaliDateTime

from main import c
from main import cn
from main import fpn , cd

def main() :
    pass

    ##
    df = pd.read_parquet('t1')

    ##
    df = reorder_df_cols_as_a_class_values(df , DOutstandingSharesCol)

    ##
    df = df.drop_duplicates(subset = [c.ftic , c.d , cd.os] , keep = 'last')

    ##
    msk = df.duplicated(subset = [c.ftic , c.d] , keep = False)

    df1 = df[msk]
    df = df[~ msk]

    ##
    msk = df1[cd.os].isna()
    df2 = df1[msk]

    ##

    ##

    ##

    ##
    save_df_as_prq(df , fpn.t2_1)

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
