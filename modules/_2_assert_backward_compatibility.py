"""

    """

from pathlib import Path

import pandas as pd

from main import clone_target_repo
from main import fpn

def check_complete_backward_compatibility(gdt) :
    dfo = gdt.read_data()

    df = pd.read_parquet(fpn.t1_1)

    dfo1 = dfo.merge(df , how = 'left' , indicator = True)

    assert dfo1['_merge'].eq('both').all() , 'Not backward compatible!'

def main() :
    pass

    ##
    gdt = clone_target_repo()

    ##
    check_complete_backward_compatibility(gdt)

    ##

##


if __name__ == "__main__" :
    main()
    print(f'{Path(__file__).name} Done!')

##


if False :
    pass

    ##

    ##
