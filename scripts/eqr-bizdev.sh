#!/usr/bin/sh

TODAY=`date --iso`
EQR_FTP="ftp://eqrdownload.ferc.gov/DownloadRepositoryProd/Selective/"
curl --list-only $EQR_FTP >> $GITHUB_WORKSPACE/eqr-bizdev/$TODAY-eqr-bizdev.txt
