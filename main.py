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

# namespace     %%%%%%%%%%%%%%%
c = tse_ns.Col()

# class         %%%%%%%%%%%%%%%
class GDU :
    g = tgdu.GitHubDataUrl()

    slf = tgdu.m + 'u-d-Outstanding-Shares'
    slf = resolve_github_url(slf)

    firm_ids_s = g.id_2_ftic
    nom_price_s = g.nom_price

class Dirs(DefaultDirs) :
    pass

class FPN :
    dyr = Dirs()

    # temp data files
    t0 = dyr.td / 't0.prq'
    t1_0 = dyr.td / 't1_0.prq'
    t1_1 = dyr.td / 't1_1.prq'
    t1_2 = dyr.td / 't1_2.prq'

class ColName :
    url = 'url'

    rst = 'r-status'
    one = '1'
    n = 'n-in-group'
    mkd = 'marked'
    bfil = 'bfill'
    ffil = 'ffill'

# class instances   %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
gdu = GDU()
dyr = Dirs()
fpn = FPN()
cn = ColName()

def clone_target_repo() :
    gdt = GitHubDataRepo(gdu.nom_price_s)
    gdt.clone_overwrite()
    return gdt

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
