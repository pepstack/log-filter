#!/bin/bash
# @file: update-package.sh
#
#   update and package all source files (自动清理并打包源代码).
#
# @author: master@pepstack.com
#
# @create: 2018-06-21 23:11:00
# @update: 2018-06-22
#
#######################################################################
# will cause error on macosx
_file=$(readlink -f $0)

_cdir=$(dirname $_file)
_name=$(basename $_file)

_proj=$(basename $_cdir)

_ver=$(cat $_cdir/VERSION)

#######################################################################

# Major_Version_Number.Minor_Version_Number[.Revision_Number[.Build_Number]]
# app-1.2.3_build201806221413.tar.gz

OLD_IFS="$IFS"
IFS="."
versegs=($_ver)
IFS="$OLD_IFS"

majorVer="${versegs[0]}"
minorVer="${versegs[1]}"
revisionVer="${versegs[2]}"

verno="$majorVer"."$minorVer"."$revisionVer"
buildno="build$(date +%Y%m%d%H%M)"


echo "[INFO] update date and version ..."
${_cdir}/revise.py \
    --path=$_cdir \
    --filter="python" \
    --author="master@pepstack" \
    --updver="$verno" \
    --recursive


echo "[INFO] clean all sources"
find ${_cdir} -name *.pyc | xargs rm -f
find ${_cdir}/utils -name *.pyc | xargs rm -f
find ${_cdir}/handlers -name *.pyc | xargs rm -f


pkgname="$_proj"-"$verno"_"$buildno".tar.gz

echo "[INFO] create package: $pkgname"
workdir=$(pwd)
outdir=$(dirname $_cdir)
cd $outdir
rm -rf "$pkgname"
tar -zvcf "$pkgname" --exclude="$_proj/.git" "$_proj/"
cd $workdir

if [ -f "$outdir/$pkgname" ]; then
    echo "[INFO] SUCCESS update-package: $outdir/$pkgname"
else
    echo "[ERROR] FAILED update-package: $outdir/$pkgname"
fi
