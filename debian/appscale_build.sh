#!/bin/bash
#
# This script installs AppScale on the local machine. It pulls in all the
# needed dependencies and configures them properly for AppScale.
#

# Some basic check: we need a way to install packages.
PKG_CMD="$(which apt-get)"
if [ -z "${PKG_CMD}" ]; then
    echo "Cannot find the package manager command!"
    exit 1
fi

set -e

# We need to update the package cache and list first, to ensure we don't
# get errors when installing basic packages.
echo -n "Updating package list and cache ..."
${PKG_CMD} update > /dev/null
echo "done."

# We need to make sure we have lsb-release, before we use it. On
# streamlined images (like docker) it may not be present.
if ! which lsb_release > /dev/null ; then
    echo -n "Installing lsb-release..."
    ${PKG_CMD} install -y lsb-release > /dev/null
    echo "done."
fi

# Get the release version and vendor.
export DIST="$(lsb_release -c -s)"
export VENDOR="$(lsb_release -i -s)"

cd `dirname $0`/..

# Let's check that the distribution is supported by the build script.
if [ ! -e ./debian/changelog.${DIST} ]; then
    echo "${VENDOR}/${DIST} is not supported."
    exit 1
fi

# Let's make sure we have appscale source installed.
if [ ! -e VERSION ]; then
    echo "Please checkout whole appscale branch."
    exit 1
fi

echo "Installing building environment for ${VENDOR}/${DIST}"
echo "Press Ctrl-C if this is not correct"

# Let's wait few seconds to allow a Ctrl-C if building is not desirable.
sleep 5

# Let's check if we run in a docker container.
export IN_DOCKER="no"
if grep docker /proc/1/cgroup > /dev/null ; then
    echo "Detected docker container."
    IN_DOCKER="yes"
    # Make sure we have default locale.
    ${PKG_CMD} install --force-yes locales
    locale-gen en_US en_US.UTF-8
    # Docker images miss the following.
    mkdir -p /var/run/sshd
    chmod 755 /var/run/sshd
fi

export APPSCALE_HOME_RUNTIME=`pwd`
export CONFIG_DIR="/etc/appscale"

# Wheezy does not have HAProxy in its main repositories.
if [ "${DIST}" = "wheezy" ]; then
    echo deb http://httpredir.debian.org/debian wheezy-backports main > \
      /etc/apt/sources.list.d/backports.list
    curl https://haproxy.debian.net/bernat.debian.org.gpg | apt-key add -
fi


# Ensure we have apt-add-repository. On some very small/custom builds it
# may be missing (for example docker).
if ! which apt-add-repository > /dev/null ; then
    echo -n "Installing software-properties-common..."
    ${PKG_CMD} install -y software-properties-common > /dev/null
    echo "done."
fi

# Trusty and Wheezy do not have Java 8.
case "$DIST" in
    trusty)
        apt-add-repository -y ppa:openjdk-r/ppa
        echo -n "Updating package list and cache ..."
        ${PKG_CMD} update > /dev/null
        echo "done."
        ;;
    wheezy)
        echo "This script does not automatically install Java 8 on Debian "\
        "Wheezy due to the lack of official support for OpenJDK 8 in the "\
        "distro. Since a Java 8 runtime is needed for Cassandra, please "\
        "install one manually and change the JAVA path defined in "\
        "AppDB/appscale/datastore/cassandra_env/templates/cassandra-env.sh "\
        "before starting AppScale."
        read -p "Press [Enter] to continue build."
        ;;
esac

# Ejabberd fails creating a cert on Azure because of domain name length, if
# the file exists already it will skip creating it and not fail. We use the
# certs we generate and change ejabberd's config file to use that instead.
mkdir -p /etc/ejabberd && touch /etc/ejabberd/ejabberd.pem

# This will install dependencies from control.$DIST (ie distro specific
# packages).
PACKAGES="$(find debian -regex ".*\/control\.${DIST}\$" -exec debian/package-list.sh {} +)"
if ! ${PKG_CMD} install -y --force-yes ${PACKAGES}; then
    echo "Fail to install depending packages for runtime."
    exit 1
fi

# This will remove all the conflicts packages.
PACKAGES="$(find debian -regex ".*\/control\.${DIST}\$" -exec debian/remove-list.sh {} +)"
if ! ${PKG_CMD} remove --purge -y --force-yes ${PACKAGES}; then
    echo "Fail to remove conflicting packages"
    exit 1
fi

# Java 8 and capnproto require the backports repository.
if [ "${DIST}" = "jessie" ]; then
    backports_line="deb http://ftp.debian.org/debian jessie-backports main"
    if ! grep $backports_line /etc/apt/sources.list; then
        echo $backports_line >> /etc/apt/sources.list
    fi
    apt-get update
    apt-get -t jessie-backports -y install capnproto
    apt-get -t jessie-backports -y install openjdk-8-jdk-headless
fi

# Let's make sure we use ruby 1.9.
case ${DIST} in
    wheezy)
        ${PKG_CMD} install -y ruby1.9.1 ruby1.9.1-dev rubygems1.9.1 irb1.9.1 \
            ri1.9.1 rdoc1.9.1 build-essential libopenssl-ruby1.9.1 libssl-dev \
            zlib1g-dev
        update-alternatives --install /usr/bin/ruby ruby /usr/bin/ruby1.9.1 400 \
            --slave   /usr/share/man/man1/ruby.1.gz ruby.1.gz \
                          /usr/share/man/man1/ruby1.9.1.1.gz \
            --slave   /usr/bin/ri ri /usr/bin/ri1.9.1 \
            --slave /usr/bin/irb irb /usr/bin/irb1.9.1 \
            --slave /usr/bin/rdoc rdoc /usr/bin/rdoc1.9.1
        update-alternatives --install /usr/bin/gem gem /usr/bin/gem1.9.1 400
        ;;
esac

# Since the last step in appscale_build.sh is to create the certs directory,
# its existence indicates that appscale has already been installed.
if [ -d ${CONFIG_DIR}/certs ]; then
    # Version 2.3.1 and prior didn't have /etc/appscale/VERSION.
    WHERE_IS_VERSION="${CONFIG_DIR}/VERSION"
    if [ ! -e ${WHERE_IS_VERSION} ]; then
        WHERE_IS_VERSION="appscale/VERSION"
    fi
    APPSCALE_MAJOR="$(sed -n 's/.*\([0-9]\)\+\.\([0-9]\)\+\.[0-9]/\1/gp' ${WHERE_IS_VERSION})"
    APPSCALE_MINOR="$(sed -n 's/.*\([0-9]\)\+\.\([0-9]\)\+\.[0-9]/\2/gp' ${WHERE_IS_VERSION})"
    if [ -z "$APPSCALE_MAJOR" -o -z "$APPSCALE_MINOR" ]; then
        echo "Cannot determine version of AppScale!"
        exit 1
    fi
    echo
    echo "Found AppScale version $APPSCALE_MAJOR.$APPSCALE_MINOR: upgrading it."
    # Make sure AppScale is not running.
    MONIT=$(which monit)
    if $MONIT summary |grep controller > /dev/null ; then
        echo "AppScale is still running: please stop it"
        [ "$FORCE_UPGRADE" = "Y" ] || exit 1
    elif echo $MONIT |grep local > /dev/null ; then
        # AppScale is not running but there is a monit
        # leftover from the custom install.
        $MONIT quit
    fi

    # This sleep is to allow the user to Ctrl-C in case an upgrade is
    # not wanted.
    echo "Upgrading AppScale version $APPSCALE_MAJOR.$APPSCALE_MINOR ..."
    sleep 5

    # Let's keep a copy of the old config: we need to move it to avoid
    # questions from dpkg.
    if [ -e /etc/haproxy/haproxy.cfg ]; then
        mv /etc/haproxy/haproxy.cfg /etc/haproxy/haproxy.cfg.appscale.old
    fi

    # Remove outdated appscale-controller and appscale-progenitor.
    if [ $APPSCALE_MAJOR -le 2 -a $APPSCALE_MINOR -le 2 ]; then
        rm -f /etc/init.d/appscale-controller
        rm -f /etc/init.d/appscale-progenitor
        update-rc.d -f appscale-progenitor remove || true
    fi

    # Remove control files we added before 1.14, and re-add the
    # default ones.
    if [ $APPSCALE_MAJOR -le 1 -a $APPSCALE_MINOR -le 14 ]; then
        rm -f /etc/default/haproxy /etc/init.d/haproxy /etc/default/monit /etc/monitrc
        if dpkg-query -l haproxy > /dev/null 2> /dev/null ; then
            ${PKG_CMD} -o DPkg::Options::="--force-confmiss" --reinstall install haproxy
        fi
        if dpkg-query -l monit > /dev/null 2> /dev/null ; then
            ${PKG_CMD} -o DPkg::Options::="--force-confmiss" --reinstall install monit
        fi
    fi

    # In version past 2.3.1 we are incompatible with ruby1.8.
fi

if [ $1 ]; then
    echo "Installing AppScale with $1 as the only supported database."
    bash debian/appscale_install.sh core || exit 1
    bash debian/appscale_install.sh $1 || exit 1
else
    echo "Installing full AppScale image"
    bash debian/appscale_install.sh all || exit 1
    bash debian/appscale.postinst || exit 1
fi

if ! mkdir -p ${CONFIG_DIR}/certs; then
    echo "Unable to complete AppScale installation."
    exit 1
fi
echo "AppScale installation completed successfully!"
