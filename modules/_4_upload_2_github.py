"""

    """

import shutil
from pathlib import Path

from githubdata import GitHubDataRepo
from persiantools.jdatetime import JalaliDateTime

from main import fpn
from main import gdu

def clone_a_repo_return_repo_obj(gd_url) :
    gdt = GitHubDataRepo(gd_url)
    gdt.clone_overwrite()
    return gdt

def replace_old_data_with_new(gdt , df_fpn) :
    gdt.data_fp.unlink()

    tjd = JalaliDateTime.now().strftime('%Y-%m-%d')
    fp = gdt.local_path / f'{tjd}.prq'

    shutil.copy(df_fpn , fp)

def push_to_github(gdt) :
    msg = 'Updated by ' + gdu.slf
    gdt.commit_and_push(msg , branch = 'main')

def main() :
    pass

    ##

    # update 0 data
    gd0 = clone_a_repo_return_repo_obj(gdu.os0_st)

    ##
    replace_old_data_with_new(gd0 , fpn.t0)

    ##
    push_to_github(gd0)

    ##

    ##
    # update final data

    gdf = clone_a_repo_return_repo_obj(gdu.os_st)

    ##
    replace_old_data_with_new(gdf , fpn.d)

    ##
    push_to_github(gdf)

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
