"""

    """

from pathlib import Path

import pandas as pd
from mirutil.df import assert_no_duplicated_rows_in_df_cols_subset
from mirutil.df import reorder_df_cols_as_a_class_values
from mirutil.df import save_df_as_prq
from namespace_mahdimir.tse import (DOutstandingSharesCol , )

from main import c
from main import cd
from main import fpn

def remove_same_ftic_and_d_but_different_os_rows(df) :
    msk1 = df.duplicated([c.ftic , c.d] , keep = False)

    msk2 = df.duplicated([c.ftic , c.d , cd.os] , keep = False)

    msk = msk1 & ~ msk2

    df = df[~ msk]

    return df

def main() :
    pass

    ##
    df = pd.read_parquet(fpn.t2_3)

    ##
    df[cd.os] = df[cd.os].astype(int)

    ##
    df = df.sort_values(c.d , ascending = False)

    ##
    df = remove_same_ftic_and_d_but_different_os_rows(df)

    ##
    save_df_as_prq(df , fpn.d0)

    ##
    df = reorder_df_cols_as_a_class_values(df , DOutstandingSharesCol)

    ##
    df = df.drop_duplicates()

    ##
    assert_no_duplicated_rows_in_df_cols_subset(df , [c.ftic , c.d])

    ##
    save_df_as_prq(df , fpn.d)

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
