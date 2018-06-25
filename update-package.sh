#!/bin/bash
# @file: update-package.sh
#
#   update and package all source files.
#
# @author: master@pepstack.com
#
# @create: 2018-06-15
# @update: 2018-06-21
#
#######################################################################
# will cause error on macosx
_file=$(readlink -f $0)

_cdir=$(dirname $_file)
_name=$(basename $_file)

_proj=$(basename $_cdir)

_ver=1.0.4
#######################################################################
echo "update date and version"
${_cdir}/revise.py \
    --path=$_cdir \
    --filter="python" \
    --author="master@pepstack" \
    --recursive

echo "clean all sources"
find ${_cdir} -name *.pyc | xargs rm -f
find ${_cdir}/utils -name *.pyc | xargs rm -f
find ${_cdir}/handlers -name *.pyc | xargs rm -f

echo "create package: $_proj-$_ver.tar.gz"
workdir=$(pwd)
outdir=$(dirname $_cdir)
cd $outdir
rm -rf "$_proj-$_ver.tar.gz"
tar -zvcf "$_proj-$_ver.tar.gz" --exclude="$_proj/.git" "$_proj/"
cd $workdir

if [ -f "$outdir/$_proj-$_ver.tar.gz" ]; then
    echo "SUCCESS update-package: $outdir/$_proj-$_ver.tar.gz"
else
    echo "FAILED update-package: $outdir/$_proj-$_ver.tar.gz"
fi
