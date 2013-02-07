\name{monet.frame}
\alias{monet.frame}
\alias{monetframe}


\title{
  Virtual R data object for MonetDB tables and query results
}
\description{
  \code{monet.frame} wraps a MonetDB table or query result and behaves like a R \code{\link[base]{data.frame}}. 
}
\usage{
  monet.frame( conn, tableOrQuery )
}
\arguments{
  \item{conn}{A MonetDB.R database connection. Created using \code{\link[DBI]{dbConnect}} with the 
  \code{\link[MonetDB.R]{MonetDB.R}} database driver.}
  \item{tableOrQuery}{A string either naming a database table or a SQL query as a basis for the new object.}
 }
\value{
  Returns a \code{monet.frame} virtual data object that behaves similar to a \code{\link[base]{data.frame}}.
}
\details{
  \code{monet.frame} wraps a MonetDB table or query result and behaves like a R \code{\link[base]{data.frame}}.
  Many operations typical for \code{data.frame} are supported, e.g. \code{$}, \code{[}, basic math and comparisons.
  
  Data is only loaded when \code{\link[MonetDB.R]{as.data.frame}} or \code{as.vector} are called, 
  and most basic operations and calculations are mapped to SQL statements. 
  This greatly reduces the amount of data that has to be transferred between MonetDB and R 
  and thereby also increases speed, allowing you to analyse even more data with R.
}
\seealso{
  \code{\link[base]{data.frame}}
}
\examples{
\dontrun{
# connect to MonetDB
conn <- dbConnect(MonetDB.R(), "monetdb://localhost/demo")
# write some test data
data(iris)
dbWriteTable(conn, "iris", iris)
# construct monet.frame
frame <- monet.frame(conn,"iris")
# show description
str(frame)
# select one of the colums
frame$sepal_width
# select ten rows and the first two columns
frame[1:10,1:2]
# calculate aggregation
mean(frame$sepal_width)

}}
\keyword{interface}
