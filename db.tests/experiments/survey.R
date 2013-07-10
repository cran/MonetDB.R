## with the ACS 3yr data
## 
## create table california as (select * from acs3yr where st =6) with data
## create table alabama as (select * from acs3yr where st=1) with data


### R scripts ###

library(sqlsurvey)
library(MonetDB.R)
monetdriver <- dbDriver("MonetDB.R")


svyrepmonet<-function(weights,repweights,scales=NULL,mf,mse=TRUE){
	
	if(length(repweights)==1) repweights<-grep(repweights,names(mf),value=TRUE)
	if (is.null(scales)) scales<-rep(1,length(repweights))
	rval<-list(weights=weights,repweights=repweights,scales=scales,mse=mse,frame=mf,call=sys.call())
	class(rval)<-"svyrepmonet"
	rval
	}
	
print.svyrepmonet<-function(x,...){
  cat("MonetFrame survey object with replicate weights:\n")
  print(x$call)
  invisible(x)
}

dim.svyrepmonet<-function(x) dim(x$frame)

dimnames.svyrepmonet<-function(x,...) dimnames(x$frame,...)

subset.svyrepmonet<-function(x, subset,...){
	x$frame<-subset(x$frame,subset)
	x
	}
	
svymean.svyrepmonet<-function(x, design,na.rm=TRUE,  ...){

	vars<-all.vars(x)
	mf<-design$frame
	
	p<-length(vars)
	m<-length(design$repweights)
	means<-numeric(p)
	repmeans<-matrix(0,nrow=m,ncol=p)
	
	wts<-design$frame[,design$weights]
	totwt<-sum(wts)
	for(i in 1:p){
	  	means[i]<-sum(mf[,vars[i]]*wts)/totwt		
		for(j in 1:m){
				repmeans[j,i] <- sum(mf[,vars[i]]*mf[,design$repweights[j]])/sum(mf[,design$repweights[[j]]])
			}
	}
	covmat<-svrVar(repmeans, 1, design$scales,mse=design$mse,coef=means)
	attr(means,"var")<-covmat
	means		
}


test <- function(table,csv) {
	print(table)

	print("sqlsurvey")
	alacs<-sqlrepsurvey("pwgtp",paste("pwgtp",1:80,sep=""),scale=4/80,rscales=rep(1,80), mse=TRUE,database="monetdb://localhost/acs", driver=monetdriver,key="idkey", user="monetdb",password="monetdb",table.name=table,check.factors=NULL)
	print("time")
	print(system.time(svymean(~agep, alacs, se=TRUE)))
	print("traffic")
	print(monetdbGetTransferredBytes())

	print("monet.frame")
	almf<-monet.frame(dbConnect(MonetDB.R(),"monetdb://localhost/acs"), table)
	set.debug(almf,FALSE)
	svyalmf<-svyrepmonet(weights="pwgtp",repweights="pwgtp[1-9]+", mse=TRUE, scales=rep(4/80,80), mf=almf)
	print("time")
	print(system.time(svymean(~agep,svyalmf)))
	print("traffic")
	print(monetdbGetTransferredBytes())

	print("read.table")
	print(system.time(alabama<-read.table(csv,header=TRUE,sep=",")))
	names(alabama) <- tolower(names(alabama))
	svyal<-svrepdesign(weights=~pwgtp,repweights="pwgtp[1-9]+", mse=TRUE, scale=1,rscales=rep(4/80,80), data=alabama)
	print("time")
	print(system.time(svymean(~agep,svyal)))
}


test("alabama","/Users/hannes/Documents/eclipse-ws/MonetDB.R/db.tests/experiments/alabama.csv")
#test("california","/export/scratch2/hannes/acsdata/california.csv")
#test("acs3yr","/export/scratch2/hannes/acsdata/ss10.all.csv")


