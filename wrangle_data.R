library(dplyr)
library(stringr)

# Import training set.
train_2013 <- read.csv("train_2013.csv", stringsAsFactors = FALSE) %>%
  tbl_df()

# The data as provided has multiple values per cell separated by spaces.
# This is because there are multiple observations within each time block.
# We need to rearrange the data frame such that each observation has its own row.

# Function to find out how many observations there are in each row of a given data set.
NumberOfObservations <- function(df) {
  num.obsv <- rep(NA, nrow(df))
  for (i in 1:nrow(df)) {
    num.row.obsv <-
      df[i, 2] %>%
      as.character() %>%
      str_split(pattern = " ") %>%
      unlist() %>%
      length()
    num.obsv[i] <- num.row.obsv
  }
  return(num.obsv)
}

# Function to take a data frame in the original format, clean it, and return the cleaned data frame.
CleanDataFrame <- function(df) {

  # Create an empty data frame to receive the cleaned data.
  num.obsv <- NumberOfObservations(df)
  new.df <- data.frame(matrix(ncol = 20, nrow = sum(num.obsv)))
  colnames(new.df) <- colnames(df)
  
  # Transplant the data into the new data frame.
  num.row <- nrow(df)
  num.col <- ncol(df)
  cumulative.num.obsv <- c(0, cumsum(num.obsv))
  for (i in 1:num.row) {
    # Transfer the "Id" data into the new data frame.
    new.df[(cumulative.num.obsv[i] + 1):cumulative.num.obsv[i + 1], 1] <- df[i, 1]
    # Transfer the "Expected" data into the new data frame.
    new.df[(cumulative.num.obsv[i] + 1):cumulative.num.obsv[i + 1], 20] <- df[i, 20]
    # Note that the first and 20th columns are the only columns without multiple values in each cell.

    for (j in 2:(num.col - 1)) {
      new.df[(cumulative.num.obsv[i] + 1):cumulative.num.obsv[i + 1], j] <-
        df[i, j] %>%
        as.character() %>%
        str_split(pattern = " ") %>%
        unlist()
    }
  }
  return(new.df)
}

# We have the function, let's run it on the data set!
# On a random subset of 1% of the data set, this took my computer 110 seconds to execute.
# I will be using a cloud instance hosted by Domino Data Lab (http://www.dominodatalab.com) to run the actual script.
cleaned.train_2013 <- CleanDataFrame(train_2013)

# Export cleaned data as a CSV file.
write.csv(cleaned.train_2013, file = "cleaned_train_2013.csv", row.names = FALSE)
