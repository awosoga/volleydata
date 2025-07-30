import requests
import os
from dotenv import load_dotenv
from io import StringIO
import json
from github import Github


def upload_to_releases(file_path, tag):
    """
    Uploads a file to a GITHUB release specified by tag.

    Parameters
    ----------
    file_path : str
        
    tag : str
        Github release tag
    """
    load_dotenv()

    token = os.getenv("GITHUB_TOKEN")
    owner = os.getenv("GITHUB_OWNER")
    repo = os.getenv("GITHUB_REPO")
    
    g = Github(token)
    repo_name = f"{owner}/{repo}"
    repo = g.get_repo(repo_name)

    release = repo.get_release(tag)

    filename = os.path.basename(file_path)

    for asset in release.get_assets():
        if asset.name == filename:
            asset.delete_asset()
            break
        
    release.upload_asset(file_path)