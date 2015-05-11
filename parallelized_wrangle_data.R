# This script is no longer up-to-date as I was getting poor performance from the parallelization.
# I have decided to shelf it for now, and concentrate on making wrangle_data.R better.

# This script is essentially wrangle_data.R modified such that it will make use of multiple cores.
# I elected not to use the piping operator from dplyr to save time on loading the package in each parallel session of R.

library(stringr)
library(data.table)
library(foreach)
library(doParallel)
library(parallel)

# Initialize some parallel processes.
numCores <- detectCores()
cl <- makeCluster(numCores)
registerDoParallel(cl)

# Import training set.
train_2013 <- fread("train_2013.csv", stringsAsFactors = FALSE)

# The data as provided has multiple values per cell separated by spaces.
# This is because there are multiple observations within each time block.
# We need to rearrange the data frame such that each observation has its own row.

# Function to find out how many observations there are in each row of a given data set.
ParallelNumberOfObservations <- function(df) {
  num.obsv <- foreach(i = 1:nrow(df), .packages = "stringr", .combine = "c") %dopar% {
    length(unlist(str_split(as.character(df[i, 2]), pattern = " ")))
  }
  return(num.obsv)
}

# Function to take a data frame in the original format, clean it, and return the cleaned data frame.
ParallelCleanDataFrame <- function(df) {
  
  # I need a vector of the number of observations in each row of the data frame.
  num.obsv <- ParallelNumberOfObservations(df)

  # Transplant the data into a new matrix.
  num.row <- nrow(df)
  num.col <- ncol(df)
  cumulative.num.obsv <- c(0, cumsum(num.obsv))
  new.df <- foreach(j = 1:num.col, .packages = "stringr", .combine = "cbind") %:%
    foreach(i = 1:num.row, .packages = "stringr", .combine = "c") %dopar% {
      if (j %in% c(1, num.col))
        rep(df[i, j], num.obsv[i])
      else
        unlist(str_split(as.character(df[i, j]), pattern = " "))
    }
  
  # Convert the matrix into a data frame and add column names.
  colnames(new.df) <- colnames(df)
  new.df <- data.frame(new.df)
  return(new.df)
}

# We have the function, let's run it on the data set!
# On a random subset of 0.1% of the data set, this took my computer 10 seconds to execute.
# I will be using a cloud instance hosted by Domino Data Lab (http://www.dominodatalab.com) to run the actual script.
cleaned.train_2013 <- ParallelCleanDataFrame(train_2013)

# Export cleaned data as a CSV file.
write.csv(cleaned.train_2013, file = "cleaned_train_2013.csv", row.names = FALSE)
