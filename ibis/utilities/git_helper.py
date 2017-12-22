"""Run git commands on git repo.
IMPORTANT: Tested on git version 1.7.1
1. Clone a git repo
2. Checkout branch
3. Commit files
4. Push files to remote
"""

import os
import subprocess
from ibis.custom_logging import get_logger


class GitCmd(object):
    """Handles git commands"""

    def __init__(self, cfg_mgr, git_dir, git_url):
        """init
        Args:
            cfg_mgr: instance of ibis.utilities.config_manager.ConfigManager
            git_dir: directory where repo will be cloned
            git_url: git url for the repo
        """
        self.cfg_mgr = cfg_mgr
        self.logger = get_logger(self.cfg_mgr)
        self.git_dir = git_dir
        self.git_url = git_url

    def get_git_command(self):
        """Build git command for running"""
        return ['git', '--git-dir=' + self.git_dir + '.git',
                '--work-tree=' + self.git_dir]

    def clone_repo(self):
        """Clone repo from git repo"""
        home = os.path.expanduser("~")
        # Check for git config file
        if not os.path.exists(home + '/.gitconfig'):
            print home + "/.gitconfig file doesn't exist."
            file_h = open(home + '/.gitconfig', 'wb')
            file_h.write('[user]\n')
            response = raw_input("Please enter your git email: ")
            file_h.write('\temail = {email}\n'.format(email=response))
            response = raw_input("Please enter your git name: ")
            file_h.write('\tname = {name}\n'.format(name=response))
            file_h.close()
            print 'Generated ~/.gitconfig'

        # make a shallow clone
        clone_cmd = ['git', 'clone', '--depth', '1', self.git_url,
                     self.git_dir]
        clone_returncode = subprocess.call(clone_cmd)
        return bool(clone_returncode == 0)

    def checkout_branch(self, from_branch, branch_name):
        """checkout git branch"""
        if 'dev' in from_branch:
            from_branch = 'dev'
        elif 'int' in from_branch:
            from_branch = 'int'
        elif 'prod' in from_branch:
            from_branch = 'prod'
        else:
            from_branch = from_branch

        current_dir = os.getcwd()
        git_cmd = self.get_git_command()
        show_ref_success = subprocess.call(git_cmd + ['show-ref', branch_name])
        # Check to see if the branch already exists. If exist, switch to it.
        if show_ref_success == 0:
            remote_branch_exists = True
            # Switch to the given branch
            subprocess.call(git_cmd + ['checkout', branch_name])
            # Pull from remote
            # git pull fails if current dir has no .git dir
            os.chdir(self.git_dir)
            subprocess.call(git_cmd + ['pull', 'origin', branch_name])
            os.chdir(current_dir)
        else:
            remote_branch_exists = False
            from_branch_ref = subprocess.call(
                git_cmd + ['show-ref', from_branch])
            if from_branch_ref == 1:
                # from_branch does not exists
                subprocess.call(git_cmd + ['checkout', '-b', branch_name])
            else:
                # Creates new branch if branch_name not exits
                subprocess.call(git_cmd + ['checkout', '-b', branch_name,
                                           'origin/' + from_branch])
        return remote_branch_exists

    def commit_files_push(self, branch_name, commit_message,
                          remote_branch_exists):
        """Commit files to repo and push"""
        current_dir = os.getcwd()
        git_cmd = self.get_git_command()

        # stage file changes
        subprocess.call(git_cmd + ['add', '-A'])

        # commit staged changes
        commit_returncode = subprocess.call(
            git_cmd + ['commit', '-m', commit_message])

        if commit_returncode == 1:
            # nothing to commit
            self.logger.warning('No file changed for git commit!')
            return False

        if remote_branch_exists:
            os.chdir(self.git_dir)
            subprocess.call(git_cmd + ['pull', 'origin', branch_name])
            os.chdir(current_dir)

        subprocess.call(git_cmd + ['push', 'origin', branch_name])
        return True
