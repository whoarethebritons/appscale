#!/bin/bash
#
# Simple script to install AppScale.
# Author: AppScale Team <support@appscale.com>

set -e

# Defaults values for script parameters.
APPSCALE_REPO="git://github.com/AppScale/appscale.git"
APPSCALE_TOOLS_REPO="git://github.com/AppScale/appscale-tools.git"
AGENTS_REPO="git://github.com/AppScale/appscale-agents.git"
THIRDPARTIES_REPO="git://github.com/AppScale/appscale-thirdparties.git"
APPSCALE_BRANCH="master"
APPSCALE_TOOLS_BRANCH="master"
AGENTS_BRANCH="master"
THIRDPARTIES_BRANCH="master"
GIT_TAG="last"
UNIT_TEST="N"

BRANCH_PARAM_SPECIFIED="N"
TAG_PARAM_SPECIFIED="N"

usage() {
    echo "Usage: ${0} [--repo <repo>] [--branch <branch>]"
    echo "            [--tools-repo <repo>] [--tools-branch <branch>]"
    echo "            [--agents-repo <repo>] [--agents-branch <branch>]"
    echo "            [--thirdparties-repo <repo>] [--thirdparties-branch <branch>]"
    echo "            [--tag <git-tag>]"
    echo
    echo "Be aware that tag parameter has priority over repo and branch parameters."
    echo "So if no tag, repos and branches are specified, tag 'last' will be used."
    echo "If you want to bootstrap using master branches of all repos, specify '--tag dev'"
    echo
    echo "Options:"
    echo "   --repo <repo>                   Specify appscale repo (default $APPSCALE_REPO)"
    echo "   --branch <branch>               Specify appscale branch (default $APPSCALE_BRANCH)"
    echo "   --tools-repo <repo>             Specify appscale-tools repo (default $APPSCALE_TOOLS_REPO)"
    echo "   --tools-branch <branch>         Specify appscale-tools branch (default $APPSCALE_TOOLS_BRANCH)"
    echo "   --agents-repo <repo>            Specify appscale-agents repo (default $AGENTS_REPO)"
    echo "   --agents-branch <branch>        Specify appscale-agents branch (default $AGENTS_BRANCH)"
    echo "   --thirdparties-repo <repo>      Specify appscale-thirdparties repo (default $THIRDPARTIES_REPO)"
    echo "   --thirdparties-branch <branch>  Specify appscale-thirdparties branch (default $THIRDPARTIES_BRANCH)"
    echo "   --tag <git-tag>                 Use git tag (ie 3.7.2) or 'last' to use the latest release"
    echo "                                   or 'dev' for HEAD (default ${GIT_TAG})"
    echo "   -t                              Run unit tests"
    exit 1
}

version_ge() {
    test "$(printf '%s\n' "$@" | sort -V | tail -1)" = "$1"
}


echo -n "Checking to make sure you are root..."
if [ "$(id -u)" != "0" ]; then
   echo "Failed" 1>&2
   exit 1
fi
echo "Success"

if ! id -u appscale > /dev/null 2>&1; then
  groupadd appscale
  useradd -r -m -c "AppScale system user." -g appscale -G sudo,ejabberd,haproxy,memcache,rabbitmq,zookeeper,cassandra -s /bin/bash appscale

  echo "appscale ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers
fi

# Let's get the command line arguments.
while [ $# -gt 0 ]; do
    if [ "${1}" = "--repo" ]; then
        shift; if [ -z "${1}" ]; then usage; fi
        APPSCALE_REPO="${1}"; BRANCH_PARAM_SPECIFIED="Y"
        shift; continue
    fi
    if [ "${1}" = "--branch" ]; then
        shift; if [ -z "${1}" ]; then usage; fi
        APPSCALE_BRANCH="${1}"; BRANCH_PARAM_SPECIFIED="Y"
        shift; continue
    fi
    if [ "${1}" = "--tools-repo" ]; then
        shift; if [ -z "${1}" ]; then usage; fi
        APPSCALE_TOOLS_REPO="${1}"; BRANCH_PARAM_SPECIFIED="Y"
        shift; continue
    fi
    if [ "${1}" = "--tools-branch" ]; then
        shift; if [ -z "${1}" ]; then usage; fi
        APPSCALE_TOOLS_BRANCH="${1}"; BRANCH_PARAM_SPECIFIED="Y"
        shift; continue
    fi
    if [ "${1}" = "--agents-repo" ]; then
        shift; if [ -z "${1}" ]; then usage; fi
        AGENTS_REPO="${1}"; BRANCH_PARAM_SPECIFIED="Y"
        shift; continue
    fi
    if [ "${1}" = "--agents-branch" ]; then
        shift; if [ -z "${1}" ]; then usage; fi
        AGENTS_BRANCH="${1}"; BRANCH_PARAM_SPECIFIED="Y"
        shift; continue
    fi
    if [ "${1}" = "--thirdparties-repo" ]; then
        shift; if [ -z "${1}" ]; then usage; fi
        THIRDPARTIES_REPO="${1}"; BRANCH_PARAM_SPECIFIED="Y"
        shift; continue
    fi
    if [ "${1}" = "--thirdparties-branch" ]; then
        shift; if [ -z "${1}" ]; then usage; fi
        THIRDPARTIES_BRANCH="${1}"; BRANCH_PARAM_SPECIFIED="Y"
        shift; continue
    fi
    if [ "${1}" = "--tag" ]; then
        shift; if [ -z "${1}" ]; then usage; fi
        GIT_TAG="${1}";
        if [ "${GIT_TAG}" != "dev" ]; then TAG_PARAM_SPECIFIED="Y"; fi
        shift; continue
    fi
    if [ "${1}" = "-t" ]; then
        UNIT_TEST="Y"
        shift; continue
    fi
    echo
    echo "Parameter '$1' is not recognized"
    echo
    usage
done


# Validate parameters combination
if [ "${BRANCH_PARAM_SPECIFIED}" = "Y" ] && [ "${TAG_PARAM_SPECIFIED}" = "Y" ]; then
    echo "Repo/Branch parameters can't be used if --tag parameter is specified"
    exit 1
fi

# Determine if we use official repos with tag or custom branches
if [ "${BRANCH_PARAM_SPECIFIED}" = "Y" ] || [ "${GIT_TAG}" = "dev" ]; then
    RELY_ON_TAG="N"
else
    RELY_ON_TAG="Y"
    if [ "${GIT_TAG}" = "last" ]; then
        echo "Determining the latest tag in AppScale/appscale repo"
        GIT_TAG=$(curl --fail https://api.github.com/repos/appscale/appscale/releases/latest \
                  | python -m json.tool | grep '"tag_name"' \
                  | awk -F ':' '{ print $2 }' | tr --delete ' ,"')
    fi
    VERSION="${GIT_TAG}"
fi

# At this time we expect to be installed in $HOME.
cd $HOME

echo
if [ "${RELY_ON_TAG}" = "Y" ]; then
    echo "Will be using the following github repos:"
    echo "AppScale:        ${APPSCALE_REPO} - Tag ${GIT_TAG}"
    echo "AppScale-Tools:  ${APPSCALE_TOOLS_REPO} - Tag ${GIT_TAG}"
    if version_ge ${VERSION} 3.8.0; then echo "Cloud-Agents:    ${AGENTS_REPO} - Tag ${GIT_TAG}"; fi
    if version_ge ${VERSION} 4.0.0; then echo "Thirdparties:    ${THIRDPARTIES_REPO} - Tag ${GIT_TAG}"; fi
    echo "Exit now (ctrl-c) if this is incorrect"
else
    echo "Will be using the following github repos:"
    echo "AppScale:        ${APPSCALE_REPO} - Branch ${APPSCALE_BRANCH}"
    echo "AppScale-Tools:  ${APPSCALE_TOOLS_REPO} - Branch ${APPSCALE_TOOLS_BRANCH}"
    echo "Cloud-Agents:    ${AGENTS_REPO} - Branch ${AGENTS_BRANCH}"
    echo "Thirdparties:    ${THIRDPARTIES_REPO} - Branch ${THIRDPARTIES_BRANCH}"
    echo "Exit now (ctrl-c) if this is incorrect"
fi
echo

sleep 5

# Wait up to 30 seconds for the package lists lock to become available.
lock_wait_start=$(date +%s)
printed_status=false
while fuser /var/lib/apt/lists/lock; do
    if [ "${printed_status}" = false ]; then
        echo "Waiting for another process to update package lists"
        printed_status=true
    fi
    current_time=$(date +%s)
    elapsed_time=$((current_time - lock_wait_start))
    if [ "${elapsed_time}" -gt 30 ]; then break; fi
    sleep 1
done
apt-get update

# Wait up to 2 min for the dpkg lock to become available.
lock_wait_start=$(date +%s)
printed_status=false
while fuser /var/lib/dpkg/lock; do
    if [ "${printed_status}" = false ]; then
        echo "Waiting for another process to update packages"
        printed_status=true
    fi
    current_time=$(date +%s)
    elapsed_time=$((current_time - lock_wait_start))
    if [ "${elapsed_time}" -gt 120 ]; then break; fi
    sleep 1
done
<<<<<<< HEAD
sudo apt-get install -y git
if [ ! -d /home/appscale/appscale ]; then
    # We split the commands, to ensure it fails if branch doesn't
    # exists (Precise git will not fail otherwise).
    su appscale <<EOF
git clone ${APPSCALE_REPO} /home/appscale/appscale
(cd /home/appscale/appscale; git checkout ${APPSCALE_BRANCH})

git clone ${APPSCALE_TOOLS_REPO} /home/appscale/appscale-tools
(cd /home/appscale/appscale-tools; git checkout ${APPSCALE_TOOLS_BRANCH})

git clone ${AGENTS_REPO} /home/appscale/appscale-agents
(cd /home/appscale/appscale-agents; git checkout ${AGENTS_BRANCH})
=======
apt-get install -y git
>>>>>>> 03bf9185c54443d326627dfecf866bd078dae6c9

APPSCALE_DIRS="\
    /root/appscale /root/appscale-tools /root/appscale-agents /root/appscale-thirdparties \
    /etc/appscale /opt/appscale /var/log/appscale /var/appscale /run/appscale"

<<<<<<< HEAD
# Use tags if we specified it.
if [ -n "$GIT_TAG"  ] && [  "${APPSCALE_BRANCH}" = "master" ]; then
    if [ "$GIT_TAG" = "last" ]; then
        GIT_TAG="$(cd /home/appscale/appscale; git tag | tail -n 1)"
    fi
    (cd /home/appscale/appscale; git checkout "$GIT_TAG")
    (cd /home/appscale/appscale-tools; git checkout "$GIT_TAG")
    (cd /home/appscale/appscale-agents; git checkout "$GIT_TAG")
fi
EOF
fi

# Since the last step in appscale_build.sh is to create the certs directory,
# its existence indicates that appscale has already been installed.
if [ -d /etc/appscale/certs ]; then
    UPDATE_REPO="Y"

    # For upgrade, we don't switch across branches.
    if [ "${FORCE_UPGRADE}" = "N" ] && [ "${APPSCALE_BRANCH}" != "master" ]; then
        echo "Cannot use --branch when upgrading"
        exit 1
    fi
    if [ "${FORCE_UPGRADE}" = "N"  ] && [  "${APPSCALE_TOOLS_BRANCH}" != "master" ]; then
        echo "Cannot use --tools-branch when upgrading"
        exit 1
    fi
    if [ "${FORCE_UPGRADE}" = "N"  ] && [  -z "$GIT_TAG" ]; then
        echo "Cannot use --tag dev when upgrading"
        exit 1
    fi

    APPSCALE_MAJOR="$(sed -n 's/.*\([0-9]\)\+\.\([0-9]\)\+\.[0-9]/\1/gp' /home/appscale/appscale/VERSION)"
    APPSCALE_MINOR="$(sed -n 's/.*\([0-9]\)\+\.\([0-9]\)\+\.[0-9]/\2/gp' /home/appscale/appscale/VERSION)"
    if [ -z "$APPSCALE_MAJOR" -o -z "$APPSCALE_MINOR" ]; then
        echo "Cannot determine version of AppScale!"
        exit 1
    fi

    # This is an upgrade, so let's make sure we use a tag that has
    # been passed, or the last one available. Let's fetch all the
    # available tags first.
    su appscale <<EOF
    (cd /home/appscale/appscale; git fetch ${APPSCALE_REPO} -t)
    (cd /home/appscale/appscale-tools; git fetch ${APPSCALE_TOOLS_REPO} -t)
    (cd /home/appscale/appscale-agents; git fetch ${AGENTS_REPO} -t)

    if [ "$GIT_TAG" = "last" ]; then
        GIT_TAG="$(cd /home/appscale/appscale; git tag | tail -n 1)"
        # Make sure we have this tag in the official repo.
        if ! git ls-remote --tags ${APPSCALE_REPO} | grep -F $GIT_TAG > /dev/null ; then
            echo "\"$GIT_TAG\" not recognized: use --tag to specify tag to upgrade to."
            exit 1
        fi
    fi

    # We can pull a tag only if we are on the master branch.
    CURRENT_BRANCH="$(cd /home/appscale/appscale; git branch --no-color | grep '^*' | cut -f 2 -d ' ')"
    if [ "${CURRENT_BRANCH}" != "master" ] && \
            (cd /home/appscale/appscale; git tag -l | grep $(git describe)) ; then
        CURRENT_BRANCH="$(cd /home/appscale/appscale; git tag -l | grep $(git describe))"
        if [ "${CURRENT_BRANCH}" = "${GIT_TAG}" ]; then
            echo "AppScale repository is already at the"\
                 "specified release. Building with current code."
            UPDATE_REPO="N"
        fi
    fi
EOF
    # If CURRENT_BRANCH is empty, then we are not on master, and we
    # are not on a released version: we don't upgrade then.
    if [ "${FORCE_UPGRADE}" = "N"  ] && [  -z "${CURRENT_BRANCH}" ]; then
        echo "Error: git repository is not 'master' or a released version."
        exit 1
    fi
=======
for appscale_presence_marker in ${APPSCALE_DIRS}; do
    if [ -d ${appscale_presence_marker} ] ; then
        echo "${appscale_presence_marker} already exists!"
        echo "bootstrap.sh script should be used for initial installation only."
        echo "Use upgrade.sh for upgrading existing deployment"
        echo "It can be found here: https://raw.githubusercontent.com/AppScale/appscale/master/upgrade.sh."
        exit 1
    fi
done

>>>>>>> 03bf9185c54443d326627dfecf866bd078dae6c9

if [ "${RELY_ON_TAG}" = "Y"  ]; then
    APPSCALE_TARGET="tags/${GIT_TAG}"
    TOOLS_TARGET="tags/${GIT_TAG}"
    AGENTS_TARGET="tags/${GIT_TAG}"
    THIRDPARTIES_TARGET="tags/${GIT_TAG}"
else
    APPSCALE_TARGET="${APPSCALE_BRANCH}"
    TOOLS_TARGET="${APPSCALE_TOOLS_BRANCH}"
    AGENTS_TARGET="${AGENTS_BRANCH}"
    THIRDPARTIES_TARGET="${THIRDPARTIES_BRANCH}"
fi


echo "Cloning appscale repositories"
# We split the commands, to ensure it fails if branch doesn't
# exists (Precise git will not fail otherwise).
git clone ${APPSCALE_REPO} appscale
(cd appscale; git checkout ${APPSCALE_TARGET})
VERSION=$(cat /root/appscale/VERSION | grep -oE "[0-9]+\.[0-9]+\.[0-9]+")

git clone ${APPSCALE_TOOLS_REPO} appscale-tools
(cd appscale-tools; git checkout "${TOOLS_TARGET}")

<<<<<<< HEAD
    if [ "${UPDATE_REPO}" = "Y" ]; then
        echo "Found AppScale version $APPSCALE_MAJOR.$APPSCALE_MINOR."\
             "An upgrade to the latest version available will be"\
             "attempted in 5 seconds."
        sleep 5
        su appscale <<EOF
        # Upgrade the repository. If GIT_TAG is empty, we are on HEAD.
        if [ -n "${GIT_TAG}" ]; then
            if ! (cd /home/appscale/appscale; git checkout "$GIT_TAG"); then
                echo "Please stash your local unsaved changes and checkout"\
                     "the version of AppScale you are currently using to fix"\
                     "this error."
                echo "e.g.: git stash; git checkout <AppScale-version>"
                exit 1
            fi

            if ! (cd /home/appscale/appscale-tools; git checkout "$GIT_TAG"); then
                echo "Please stash your local unsaved changes and checkout"\
                     "the version of appscale-tools you are currently using"\
                     "to fix this error."
                echo "e.g.: git stash; git checkout <appscale-tools-version>"
                exit 1
            fi
        elif [ "${FORCE_UPGRADE}" = "N" ]; then
            (cd /home/appscale/appscale; git pull)
            (cd /home/appscale/appscale-tools; git pull)
        else
            RANDOM_KEY="$(echo $(date), $$|md5sum|head -c 6)-$(date +%s)"
            REMOTE_REPO_NAME="appscale-bootstrap-\$RANDOM_KEY"
            if ! (cd /home/appscale/appscale;
                    git remote add -t "${APPSCALE_BRANCH}" -f "\$REMOTE_REPO_NAME" "${APPSCALE_REPO}";
                    git checkout "\$REMOTE_REPO_NAME"/"${APPSCALE_BRANCH}"); then
                echo "Please make sure the repository url is correct, the"\
                     "branch exists, and that you have stashed your local"\
                     "changes."
                echo "e.g.: git stash, git remote add -t {remote_branch} -f"\
                     "{repo_name} {repository_url}; git checkout"\
                     "{repo_name}/{remote_branch}"
                exit 1
            fi
            if ! (cd /home/appscale/appscale-tools;
                    git remote add -t "${APPSCALE_TOOLS_BRANCH}" -f "\$REMOTE_REPO_NAME" "${APPSCALE_TOOLS_REPO}";
                    git checkout "\$REMOTE_REPO_NAME"/"${APPSCALE_TOOLS_BRANCH}"); then
                echo "Please make sure the repository url is correct, the"\
                     "branch exists, and that you have stashed your local"\
                     "changes."
                echo "e.g.: git stash, git remote add -t {remote_branch} -f"\
                     "{repo_name} {repository_url}; git checkout"\
                     "{repo_name}/{remote_branch}"
                exit 1
            fi
        fi
EOF
    fi
=======
if [ "${RELY_ON_TAG}" = "N" ] || version_ge "${VERSION}" 3.8.0; then
    git clone ${AGENTS_REPO} appscale-agents
    (cd appscale-agents; git checkout "${AGENTS_TARGET}")
fi
if [ "${RELY_ON_TAG}" = "N" ] || version_ge "${VERSION}" 4.0.0; then
    git clone ${THIRDPARTIES_REPO} appscale-thirdparties
    (cd appscale-thirdparties; git checkout "${THIRDPARTIES_TARGET}")
>>>>>>> 03bf9185c54443d326627dfecf866bd078dae6c9
fi


echo -n "Building AppScale..."
<<<<<<< HEAD
if ! (cd /home/appscale/appscale/debian; bash appscale_build.sh) ; then
    echo "failed!"
    exit 1
fi

echo -n "Installing AppScale Agents..."
if ! (cd /home/appscale/appscale-agents/; make install-no-venv) ; then
    echo "Failed to install AppScale Agents"
    exit 1
fi

echo -n "Building AppScale Tools..." 
if ! (cd /home/appscale/appscale-tools/debian; bash appscale_build.sh) ; then
    echo "failed!"
=======
if ! (cd appscale/debian; bash appscale_build.sh) ; then
    echo "Failed to install AppScale core"
    exit 1
fi

if [ "${RELY_ON_TAG}" = "N" ] || version_ge "${VERSION}" 3.8.0; then
    echo -n "Installing AppScale Agents..."
    if ! (cd appscale-agents/; make install-no-venv) ; then
        echo "Failed to install AppScale Agents"
        exit 1
    fi
fi

echo -n "Building AppScale Tools..." 
if ! (cd appscale-tools/debian; bash appscale_build.sh) ; then
    echo "Failed to install AppScale-Tools"
>>>>>>> 03bf9185c54443d326627dfecf866bd078dae6c9
    exit 1
fi

if [ "${RELY_ON_TAG}" = "N" ] || version_ge "${VERSION}" 4.0.0; then
    echo -n "Installing Thirdparty software..."
    if ! (cd appscale-thirdparties/; bash install_all.sh) ; then
        echo "Failed to install Thirdparties software"
        exit 1
    fi
fi

# Run unit tests if asked.
if [ "$UNIT_TEST" = "Y" ]; then
    echo "Running Unit tests"
    (cd /home/appscale/appscale; rake)
    if [ $? -gt 0 ]; then
        echo "Unit tests failed for appscale!"
        exit 1
    fi
    (cd /home/appscale/appscale-tools; rake)
    if [ $? -gt 0 ]; then
        echo "Unit tests failed for appscale-tools!"
        exit 1
    fi
    echo "Unit tests complete"
fi

# Let's source the profiles so this image can be used right away.
. /etc/profile.d/appscale.sh

echo "****************************************"
echo "  AppScale is installed on the machine  "
echo "****************************************"
exit 0
