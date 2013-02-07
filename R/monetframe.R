# this wraps a sql database (in particular MonetDB) with a DBI connector 
# to have it appear like a data.frame

# show each step of rewriting the query
DEBUG_REWRITE   <- FALSE


# can either be given a query or simply a table name
monet.frame <- monetframe <- function(conn,tableOrQuery) {
	if(missing(conn)) stop("'conn' must be specified")
	if(missing(tableOrQuery)) stop("a sql query or a table name must be specified")
	
	obj = new.env()
	class(obj) = "monet.frame"
	attr(obj,"conn") <- conn
	query <- tableOrQuery
	
	if (dbExistsTable(conn,tableOrQuery)) {
		query <- paste0("SELECT * FROM ",make.db.names(conn,tableOrQuery,allow.keywords=FALSE))
	}
	# get column names and types from prepare response
	res <- dbGetQuery(conn, paste0("PREPARE ",query))
	
	attr(obj,"query") <- query
	attr(obj,"ctypes") <- res$type
	attr(obj,"cnames") <- res$column
	attr(obj,"ncol") <- length(res$column)
	attr(obj,"rtypes") <- lapply(res$type,monetdbRtype)
	
	# get result set length by rewriting to count(*), should be much faster
	attr(obj,"nrow") <- dbGetQuery(conn,sub("(SELECT )(.*?)( FROM.*)","\\1COUNT(*)\\3",query,ignore.case=TRUE))[[1,1]]
	return(obj)
}

.element.limit <- 10000000

as.data.frame.monet.frame <- function(x, row.names, optional, warnSize=TRUE,...) {
	# check if amount of tuples/fields is larger than some limit
	# raise error if over limit and warnSize==TRUE
	if (ncol(x)*nrow(x) > .element.limit && warnSize) 
		stop(paste0("The total number of elements to be loaded is larger than ",.element.limit,". This is probably very slow. Consider dropping columns and/or rows, e.g. using the [] function. If you really want to do this, call as.data.frame() with the warnSize parameter set to FALSE."))
	# get result set object from frame
	return(dbGetQuery(attr(x,"conn"),attr(x,"query")))
}

as.vector.monet.frame <- function(x,...) {
	if (ncol(x) != 1)
		stop("as.vector can only be used on one-column monet.frame objects. Consider using $.")
	as.data.frame(x)[[1]]
}

# this is the fun part. this method has infinity ways of being invoked :(
# http://stat.ethz.ch/R-manual/R-patched/library/base/html/Extract.data.frame.html

# TODO: handle negative indices and which() calls. which() like subset!

"[.monet.frame" <- function(x, k, j,drop=TRUE) {	
	nquery <- query <- getQuery(x)
	
	cols <- NA
	rows <- NA
	
	# biiig fun with nargs to differentiate d[1,] and d[1]
	# all in the presence of the optional drop argument, yuck!
	args <- nargs()
	if (!missing(drop)) {
		args <- args-1
	}
	if (args == 2 && missing(j)) cols <- k
	if (args == 3 && !missing(j)) cols <- j
	if (args == 3 && !missing(k)) rows <- k
		
	if (length(cols) > 1 || !is.na(cols)) { # get around an error if cols is a vector...
		# if we have a numeric column spec, find the appropriate names
		if (is.numeric(cols)) {
			if (min(cols) < 1 || max(cols) > ncol(x)) 
				stop(paste0("Invalid column specification '",cols,"'. Column indices have to be in range [1,",ncol(x),"].",sep=""))			
			cols <- names(x)[cols]
		}
		if (!all(cols %in% names(x)))
			stop(paste0("Invalid column specification '",cols,"'. Column names have to be in set {",paste(names(x),collapse=", "),"}.",sep=""))			
		
		nquery <- sub("SELECT.+FROM",paste0("SELECT ",paste0(make.db.names(attr(x,"conn"),cols),collapse=", ")," FROM"),query)
	}
	
	if (length(rows) > 1 || !is.na(rows)) { # get around an error if cols is a vector...
		if (min(rows) < 1 || max(rows) > nrow(x)) 
			stop("Invalid row specification. Row indices have to be in range [1,",nrow(x),"].",sep="")			
		
		if (.is.sequential(rows)) {
			# find out whether we already have limit and/or offset set
			# our values are relative to them
	
			oldLimit <- 0
			oldOffset <- 0
			
			oldOffsetStr <- gsub("(.*offset[ ]+)(\\d+)(.*)","\\2",nquery,ignore.case=TRUE)
			if (oldOffsetStr != nquery) {
				oldOffset <- as.numeric(oldOffsetStr)
			}
			
			offset <- oldOffset + min(rows)-1 # offset means skip n rows, but r lower limit includes them
			limit <- max(rows)-min(rows)+1

			# remove old limit/offset from query
			# TODO: is this safe? UNION queries are particularly dangerous, again...
			nquery <- gsub("limit[ ]+\\d+|offset[ ]+\\d+","",nquery,ignore.case=TRUE)
			nquery <- sub(";? *$",paste0(" LIMIT ",limit," OFFSET ",offset),nquery,ignore.case=TRUE)
		}
		else 
			warning(paste("row specification has to be sequential, but ",paste(rows,collapse=",")," is not. Try as.data.frame(x)[c(",paste(rows,collapse=","),"),] instead.",sep=""))
	}
	
	# this would be the only case for column selection where the 'drop' parameter has an effect.
	# we have to create a warning, since drop=TRUE is default behaviour and might be expected by users
	if (((!is.na(cols) && length(cols) == 1) || (!is.na(rows) && length(rows) == 1)) && drop) 
		warning("drop=TRUE for one-column or one-row results is not supported. Overriding to FALSE")
	
	# construct and return new monet.frame for rewritten query

	if (DEBUG_REWRITE)  cat(paste0("RW: '",query,"' >> '",nquery,"'\n",sep=""))	

	monet.frame(attr(x,"conn"),nquery)
}

.is.sequential <- function(x, eps=1e-8) {
	if (length(x) && isTRUE(abs(x[1] - floor(x[1])) < eps)) {
		all(abs(diff(x)-1) < eps)
	} else {
		FALSE
	}
}

# shorthand for frame[columnname/id,drop=FALSE]
"$.monet.frame"<-function(x,i) {
	x[i,drop=FALSE]
}

# returns a single row with one index/element with two indices
"[[.monet.frame"  <- function(x, j, ...) {
	print("[[.monet.frame - Not implemented yet.")
	FALSE
}



str.monet.frame <- summary.monet.frame <- function(object, ...) {
	cat("MonetDB-backed data.frame surrogate\n")
	# i agree this is overkill, but still...
	nrows <- nrow(object)
	ncols <- ncol(object)
	rowsdesc <- "rows"
	if (nrows == 1) rowsdesc <- "row"
	colsdesc <- "columns"
	if (ncols == 1) colsdesc <- "column"
	cat(paste0(ncol(object)," ",colsdesc,", ",nrow(object)," ",rowsdesc,"\n"))
	
	cat(paste0("Query: ",getQuery(object),"\n"))	
	str(as.data.frame(object[1:6,,drop=FALSE]))
}

# TODO: do something more clever here?
print.monet.frame <- function(x, ...) {
	print(as.data.frame(x))
}

names.monet.frame <- function(x) {
	attr(x,"cnames")
}

dim.monet.frame <- function(x) {
	c(attr(x,"nrow"),attr(x,"ncol"))
}

# TODO: fix issue with constant values, subset(x,foo > "bar")

# http://stat.ethz.ch/R-manual/R-patched/library/base/html/subset.html
subset.monet.frame<-function(x,subset,...){
	query <- getQuery(x)
	subset<-substitute(subset)
	restr <- sqlexpr(subset)

	if (length(grep(" where ",query,ignore.case=TRUE)) > 0) {
		nquery <- sub("where (.*?) (group|having|order|limit|;)",paste0("where \\1 AND ",restr," \\2"),query,ignore.case=TRUE)
	}
	else {
		nquery <- sub("(group|having|order|limit|;|$)",paste0(" where ",restr," \\1"),query,ignore.case=TRUE)
	}

	if (DEBUG_REWRITE)  cat(paste0("RW: '",query,"' >> '",nquery,"'\n",sep=""))	
	
	# construct and return new monet.frame for rewritten query
	monet.frame(attr(x,"conn"),nquery)	
}

# basic math and comparision operators
#  ‘"+"’, ‘"-"’, ‘"*"’, ‘"/"’, ‘"^"’, ‘"%%"’, `"%/%"’ (only numeric)
#  ‘"&"’, ‘"|"’, ‘"!"’ (only boolean)
#  ‘"<"’, ‘"<="’, ‘">="’, ‘">"’  (only numeric)
#  ‘"=="’, ‘"!="’ (generic)

Ops.monet.frame <- function(e1,e2) {
	unary <- nargs() == 1L
	lclass <- nzchar(.Method[1L])
	rclass <- !unary && (nzchar(.Method[2L]))
	
	# this will be the next SELECT x thing
	nexpr <- NA
	
	left <- right <- query <- queryresult <- conn <- NA
	leftNum <- rightNum <- leftBool <- rightBool <- NA
	
	# both values are monet.frame
	if (lclass && rclass) {
		if (any(dim(e1) != dim(e2)) || ncol(e1) != 1 || ncol(e2) != 1) 
			stop(.Generic, " only defined for one-column result sets of equal length.")
		
		lquery <- query <- getQuery(e1)
		conn <- attr(e1,"conn")
		
		rquery <- getQuery(e2)
		
		left <- sub("(select )(.*?)( from.*)","(\\2)",lquery,ignore.case=TRUE)
		right <- sub("(select )(.*?)( from.*)","(\\2)",rquery,ignore.case=TRUE)
		
		leftrem <- sub("(select )(.*?)( from.*)","(\\1)X(\\3)",lquery,ignore.case=TRUE)
		rightrem <- sub("(select )(.*?)( from.*)","(\\1)X(\\3)",rquery,ignore.case=TRUE)
		
		if (leftrem != rightrem) {
			stop("left and right columns have to come from the same table with the same restrictions.")
		}
		
		# some tests for data types
				
		leftNum <- attr(e1,"rtypes")[[1]] == "numeric"
		leftBool <- attr(e1,"rtypes")[[1]] == "logical"
		rightNum <- attr(e2,"rtypes")[[1]] == "numeric"
		rightBool <- attr(e2,"rtypes")[[1]] == "logical"
	}
	
	# left operand is monet.frame
	else if (lclass) {
		if (ncol(e1) != 1) 
			stop(.Generic, " only defined for one-column frames, consider using $ first")
		if (length(e2) != 1)
			stop("Only single-value constants are supported.")
		query <- getQuery(e1)
		conn <- attr(e1,"conn")
				
		left <- sub("(select )(.*?)( from.*)","(\\2)",query,ignore.case=TRUE)
	
		leftNum <- attr(e1,"rtypes")[[1]] == "numeric"
		leftBool <- attr(e1,"rtypes")[[1]] == "logical"
		
		right <- e2
		rightNum <- is.numeric(e2)
		rightBool <- is.logical(e2)		
	}
	
	# right operand is monet.frame
	else {
		if (ncol(e2) != 1) 
			stop(.Generic, " only defined for one-column frames, consider using $ first")
		if (length(e1) != 1)
			stop("Only single-value constants are supported.")
		query <- getQuery(e2)
		
		right <- sub("(select )(.*?)( from.*)","(\\2)",query,ignore.case=TRUE)
		
		conn <- attr(e2,"conn")
		
		rightNum <- attr(e2,"rtypes")[[1]] == "numeric"
		rightBool <- attr(e2,"rtypes")[[1]] == "logical"
		
		left <- e1
		leftNum <- is.numeric(e1)
		leftBool <- is.logical(e1)
	}
	
	if (DEBUG_REWRITE)  cat(paste0("OP: ",.Generic," on ",left,", ",right,"\n",sep=""))	
	
	# mapping of R operators to DB operators...booring		
	if (.Generic %in% c("+", "-", "*", "/","<",">","<=",">=")) {
		if (!leftNum || !rightNum)
			stop(.Generic, " only supported for numeric arguments")
		nexpr <- paste0(left,.Generic,right)
	}
	if (.Generic == "^") {
		if (!leftNum || !rightNum)
			stop(.Generic, " only supported for numeric arguments")
		nexpr <- paste0("POWER(",left,",",right,")")
	}
	if (.Generic == "%%") {
		if (!leftNum || !rightNum)
			stop(.Generic, " only supported for numeric arguments")
		nexpr <- paste0(left,"%",right)
	}
	
	if (.Generic == "%/%") {
		if (!leftNum || !rightNum)
			stop(.Generic, " only supported for numeric arguments")
		nexpr <- paste0(left,"%CAST(",right," AS BIGINT)")
	}
	
	if (.Generic == "!") {
		if (!leftBool)
			stop(.Generic, " only supported for logical (boolean) arguments")
		nexpr <- paste0("NOT(",left,")")
	}
	
	if (.Generic == "&") {
		if (!leftBool || !rightBool)
			stop(.Generic, " only supported for logical (boolean) arguments")
		nexpr <- paste0(left," AND ",right)
	}
	
	if (.Generic == "|") {
		if (!leftBool || !rightBool)
			stop(.Generic, " only supported for logical (boolean) arguments")
		nexpr <- paste0(left," OR ",right)
	}
	
	if (.Generic == "==") {
		nexpr <- paste0(left,"=",right)
	}
	
	if (.Generic == "!=") {
		nexpr <- paste0("NOT(",left,"=",right,")")
	}
		
	if (is.na(nexpr)) 
		stop(.Generic, " not supported (yet). Sorry.")
	
	# replace the thing between SELECT and WHERE with the new value and return new monet.frame
	nquery <- sub("select (.*?) from",paste0("SELECT ",nexpr," FROM"),query,ignore.case=TRUE)
		
	if (DEBUG_REWRITE)  cat(paste0("RW: '",query,"' >> '",nquery,"'\n",sep=""))	
	
	# construct and return new monet.frame for rewritten query
	monet.frame(conn,nquery)	
}

# works: min/max/sum/range
# TODO: implement  ‘all’, ‘any’, ‘prod’ (product)
Summary.monet.frame <- function(x,na.rm=FALSE,...) {
	as.data.frame(.col.func(x,.Generic))[[1,1]]
}

mean.monet.frame <- avg.monet.frame <- function(x,...) {
	as.data.frame(.col.func(x,"avg"))
}

.col.func <- function(x,func,extraarg=""){
	if (ncol(x) != 1) 
		stop(func, " only defined for one-column frames, consider using $ first.")
	
	if (attr(x,"rtypes")[[1]] != "numeric")
		stop(names(x), " is not a numerical column.")
	
	query <- getQuery(x)
	col <- sub("(select )(.*?)( from.*)","\\2",query,ignore.case=TRUE)
	
	if (DEBUG_REWRITE)  cat(paste0("OP: ",func," on ",col,"\n",sep=""))	
	
	conn <- attr(x,"conn")
	nexpr <- NA
	
	if (func %in% c("min", "max", "sum","avg","abs","sign","sqrt","floor","ceiling","exp","log","cos","sin","tan","acos","asin","atan","cosh","sinh","tanh")) {
		nexpr <- paste0(toupper(func),"(",col,")")
	}
	if (func == "range") {
		return(c(.col.func(x,"min"),.col.func(x,"max")))
	}
	
	if (func == "round") {
		nexpr <- paste0("ROUND(",col,",",extraarg,")")
	}
	if (func == "signif") {
		# in SQL, ROUND(123,-1) will zero 1 char from the rear (120), 
		# in R, signif(123,1) will start from the front (100)
		# so, let's adapt
		nexpr <- paste0("ROUND(",col,",-1*LENGTH(",col,")+",extraarg,")")
	}
		
	if (is.na(nexpr)) 
		stop(func, " not supported (yet). Sorry.")
	
	# replace the thing between SELECT and WHERE with the new value and return new monet.frame
	nquery <- sub("select (.*?) from",paste0("SELECT ",nexpr," FROM"),query,ignore.case=TRUE)
	
	# clear previous result set to free db resources waiting for fetch()
	
	if (DEBUG_REWRITE)  cat(paste0("RW: '",query,"' >> '",nquery,"'\n",sep=""))	
	
	# construct and return new monet.frame for rewritten query
	monet.frame(conn,nquery)
}


# TODO: implement remaining operations: expm1, log1p, *gamma, cum*
# Or just fallback to local calculation?
Math.monet.frame <- function(x,digits=0,...) {
	# yeah, baby...
	if (.Generic == "acosh") {
		return(log(x + sqrt(x^2-1)))
	}
	if (.Generic == "asinh") {
		return(log(x + sqrt(x^2+1)))
	}
	if (.Generic == "atanh") {
		return(0.5*log((1+x)/(1-x)))
	}
	if (.Generic == "round") {
		return(.col.func(x,"round",digits))	
	}
	if (.Generic == "trunc") {
		return(.col.func(x,"round",0))	
	}
	if (.Generic == "signif") {
		return(.col.func(x,"signif",digits))	
	}
	return(.col.func(x,.Generic))
}

# 'borrowed' from sqlsurvey, translates a subset() argument to sqlish
sqlexpr<-function(expr, design){
	nms<-new.env(parent=emptyenv())
	assign("%in%"," IN ", nms)
	assign("&", " AND ", nms)
	assign("=="," = ",nms)
	assign("|"," OR ", nms)
	assign("!"," NOT ",nms)
	assign("I","",nms)
	assign("~","",nms)
	out <-textConnection("str","w",local=TRUE)
	inorder<-function(e){
		if(length(e) ==1) {
			cat(e, file=out)
		} else if (e[[1]]==quote(is.na)){
			cat("(",file=out)
			inorder(e[[2]])
			cat(") IS NULL", file=out)
		} else if (length(e)==2){
			nm<-deparse(e[[1]])
			if (exists(nm, nms)) nm<-get(nm,nms)
			cat(nm, file=out)
			cat("(", file=out)
			inorder(e[[2]])
			cat(")", file=out)
		} else if (deparse(e[[1]])=="c"){
			cat("(", file=out)
			for(i in seq_len(length(e[-1]))) {
				if(i>1) cat(",", file=out)
				inorder(e[[i+1]])
			}
			cat(")", file=out)
		} else if (deparse(e[[1]])==":"){
			cat("(",file=out)
			cat(paste(eval(e),collapse=","),file=out)
			cat(")",file=out)
		} else{
			cat("(",file=out)
			inorder(e[[2]])
			nm<-deparse(e[[1]])
			if (exists(nm,nms)) nm<-get(nm,nms)
			cat(nm,file=out)
			inorder(e[[3]])
			cat(")",file=out)
		}
		
	}
	inorder(expr)
	close(out)
	paste("(",str,")")
	
}

getQuery <- function(x) {
	attr(x,"query")
}

`[<-.monet.frame` <- `dim<-.monet.frame` <- `dimnames<-.monet.frame` <- `names<-.monet.frame` <- function(x, j, k, ..., value) {
	stop("write operators not (yet) supported for monet.frame")
}
