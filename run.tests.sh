#!/bin/sh
RCMD="R --vanilla --slave -f "
#echo "svn up"
#svn up 2> /dev/null
echo "installing latest version"
R CMD INSTALL . 2> /dev/null 
echo "monetdb.test.R"
$RCMD db.tests/monetdb.test.R 2> /dev/null
echo "sqlsurvey.test.R"
$RCMD db.tests/sqlsurvey.test.R 2> /dev/null
echo "monetframe.test.R"
$RCMD db.tests/monetframe.test.R 2> /dev/null
