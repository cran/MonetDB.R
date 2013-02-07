

acsdir <- "/export/scratch2/hannes/acsdata/"

table <- "alabama"
csv <- paste0(acsdir,"ss10pal.csv")


testanalysis <- function(surveyobj) {
	list(st1=svytotal(~sex,surveyobj),st2=
	svytotal(~cut(agep,c(4,9,14,19,24,34,44,54,59,64,74,84)),surveyobj),st3=
	svymean(~wagp,surveyobj,byvar=~sex))
}

#library(sqlsurvey)
#library(MonetDB.R)

# sqlsurvey version
#acs.sql<-sqlrepsurvey("pwgtp",paste("pwgtp",1:80,sep=""),scale=4/80,rscales=rep(1,80), mse=TRUE,database="monetdb://localhost:50000/acs", driver=dbDriver("MonetDB"),key="idkey",user="monetdb",password="monetdb",table.name=table)
#testanalysis(acs.sql)
#works so far

#
library(survey)

r <- read.table(csv,header=TRUE,sep=",")
names(r) <- tolower(names(r))

acs.svy<-svrepdesign(data=r,repweights="pwgtp[1-8]",scale=4/80,rscales=80,mse=TRUE)
testanalysis(acs.svy)

remove(r)


## monet.frame version
#con <- dbConnect(dbDriver("MonetDB"), "monetdb://localhost:50000/acs", "monetdb", "monetdb",timeout=100)
#acs.mfr<-svydesign(id=~dnum, weights=~pw, data=monet.frame(con,table), fpc=~fpc)
#testanalysis(acs.mfr)
#
#
#
## TODO: collect stats