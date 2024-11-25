import os
import shutil
from git import Repo


def refresh_holidays():
    if os.path.exists("data"):
        shutil.rmtree("data")

    Repo.clone_from("git@github.com:openpotato/openholidaysapi.data.git", "data")


refresh_holidays()
