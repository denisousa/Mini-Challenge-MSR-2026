import os
from pathlib import Path
from typing import Union, Dict, Any, List
from git import Repo
from clone_genealogy.utils import safe_rmtree
from clone_genealogy.prints_operations import printInfo, printWarning
from datetime import datetime
from time import sleep

def clean_git_locks(repo_path: Union[str, Path]) -> None:
    """Remove Git lock files that may prevent operations."""
    repo_path = Path(repo_path)
    git_dir = repo_path / '.git'
    
    if not git_dir.exists():
        return
    
    # Common Git lock files
    lock_files = [
        git_dir / 'index.lock',
        git_dir / 'HEAD.lock',
        git_dir / 'config.lock',
        git_dir / 'shallow.lock',
    ]
    
    for lock_file in lock_files:
        if lock_file.exists():
            try:
                lock_file.unlink()
                print(f"Removed lock file: {lock_file}")
            except Exception as e:
                print(f"Warning: Could not remove lock file {lock_file}: {e}")
    
    # Check refs directory for lock files
    refs_dir = git_dir / 'refs'
    if refs_dir.exists():
        for lock_file in refs_dir.rglob('*.lock'):
            try:
                lock_file.unlink()
                print(f"Removed lock file: {lock_file}")
            except Exception as e:
                print(f"Warning: Could not remove lock file {lock_file}: {e}")

def SetupRepo(ctx: "Context"):
    git_url, paths = ctx.git_url, ctx.paths

    print("Setting up local directory for git repository " + git_url)

    repo_git_dir = os.path.join(paths.repo_dir, ".git")

    if os.path.isdir(repo_git_dir):
        # Open with GitPython and fetch/pull safely (cross‑platform)
        repo = Repo(paths.repo_dir)
        try:
            # Clean Git locks before operations
            clean_git_locks(paths.repo_dir)
            
            # Fetch all remotes
            for remote in repo.remotes:
                remote.fetch(prune=True)
            # Try fast-forward pull on active branch (if not detached)
            if not repo.head.is_detached:
                try:
                    repo.git.pull("--ff-only")
                except Exception:
                    printInfo("Pull --ff-only skipped (non-FF or no upstream). Fetched refs only.")
            else:
                printInfo("Detached HEAD or no branch; fetched refs only.")
        except Exception as e:
            printWarning(f"Git fetch/pull encountered an issue: {e}")
        return

    # Not a git repo but folder exists → clean it
    if os.path.isdir(paths.repo_dir):
        clean_git_locks(paths.repo_dir)
        safe_rmtree(paths.repo_dir)

    # Clone fresh (GitPython)
    os.makedirs(paths.ws_dir, exist_ok=True)
    Repo.clone_from(git_url, paths.repo_dir)
    print(" Repository setup complete.\n")

def GetHashes(ctx: "Context") -> List[str]:
    paths = ctx.paths
    hashes: List[str] = []
    if not os.path.exists(paths.hist_file):
        return hashes
    with open(paths.hist_file, "rb") as fp:
        for raw in fp:
            raw = raw.strip()
            if not raw:
                continue
            first = raw.split(None, 1)[0]
            try:
                h = first.decode("ascii")
            except UnicodeDecodeError:
                continue
            hashes.append(h)
    hashes.reverse()
    return hashes

def GitCheckout(repo, current_hash, ctx):
    paths = ctx.paths
    try:
        head_short = repo.git.rev_parse("--short", "HEAD")
    except Exception:
        head_short = ""
    if current_hash not in head_short:
        try:
            # Clean Git locks before checkout
            clean_git_locks(paths.repo_dir)
            repo.git.checkout(current_hash, f=True)
        except Exception as e:
            # Retry once after cleaning locks
            try:
                clean_git_locks(paths.repo_dir)
                sleep(1)
                
                repo.git.checkout(current_hash, f=True)
            except Exception:
                raise RuntimeError(f"git checkout {current_hash} failed: {e}")
        sleep(0.5)