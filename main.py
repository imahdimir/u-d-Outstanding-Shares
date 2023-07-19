"""

    """

from pathlib import Path

from giteasy.github_repo import resolve_github_url
from githubdata import GitHubDataRepo
from mirutil.dirr import DefaultDirs
from mirutil.run_modules import clean_cache_dirs
from mirutil.run_modules import run_modules_from_dir_in_order
from namespace_mahdimir import tse as tse_ns
from namespace_mahdimir import tse_github_data_url as tgdu
from namespace_mahdimir.tse import D0OutstandingSharesCol

# namespace     %%%%%%%%%%%%%%%
c = tse_ns.Col()
cd = D0OutstandingSharesCol()

# class         %%%%%%%%%%%%%%%
class GDU :
    g = tgdu.GitHubDataUrl()

    slf = tgdu.m + 'u-d-Outstanding-Shares'
    slf = resolve_github_url(slf)

    firm_ids_s = g.id_2_ftic
    nom_price_s = g.nom_price

    os0_st = g.os0
    os_st = g.os

class Dirs(DefaultDirs) :
    pass

class FPN :
    dyr = Dirs()

    # temp data files
    t0 = dyr.td / 't0.prq'
    t1 = dyr.td / 't1.prq'
    t2_0 = dyr.td / 't2_0.prq'
    t2_1 = dyr.td / 't2_1.prq'
    t2_2 = dyr.td / 't2_2.prq'
    t2_3 = dyr.td / 't2_3.prq'

class ColName :
    url = 'url'

    rst = 'r-status'
    one = '1'
    n = 'n-in-group'
    mkd = 'marked'
    bfil = 'bfill'
    ffil = 'ffill'
    isna = 'isna'

# class instances   %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
gdu = GDU()
dyr = Dirs()
fpn = FPN()
cn = ColName()

def count_nan_os_values(df) :
    msk = df[cd.os].isna()
    print('Nan os count:' , len(msk[msk]))

def main() :
    pass

    ##
    run_modules_from_dir_in_order(dyr.md)

    ##
    clean_cache_dirs()

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

##
