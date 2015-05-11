library(dplyr)
library(stringr)
library(data.table)

# Import test set.
test_2014 <- fread("test_2014.csv", stringsAsFactors = FALSE) %>%
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
  
  num.row <- nrow(df)
  num.col <- ncol(df)
  
  # Create an empty data frame to receive the cleaned data.
  num.obsv <- NumberOfObservations(df)
  new.df <- data.frame(matrix(ncol = num.col, nrow = sum(num.obsv)))
  colnames(new.df) <- colnames(df)
  
  # Transplant the data into the new data frame.
  cumulative.num.obsv <- c(0, cumsum(num.obsv))
  for (i in 1:num.row) {
    # Transfer the "Id" data into the new data frame.
    new.df[(cumulative.num.obsv[i] + 1):cumulative.num.obsv[i + 1], 1] <- df[i, 1]
    # Note that the first column is the only columns without multiple values in each cell.
    
    for (j in 2:num.col) {
      new.df[(cumulative.num.obsv[i] + 1):cumulative.num.obsv[i + 1], j] <-
        df[i, j] %>%
        as.character() %>%
        str_split(pattern = " ") %>%
        unlist()
    }
  }
  return(new.df)
}

RemoveBadData <- function(df) {
  
  num.col <- ncol(df)
  
  # Convert entries to numeric type.
  for (i in 1:num.col) {
    df[, i] <- df[, i] %>% as.numeric()
  }
  
  # Set "missing data" as NA.
  df[df == -99900] <- NA
  df[df == -99901] <- NA
  df[df == -99903] <- NA
  df[df == 999] <- NA
  
  # HydrometeorType is a factor, not numeric.
  df$HydrometeorType <- as.factor(df$HydrometeorType)
  
  return(df)
}

# We have the functions, let's run it on the data set!
# I will be splitting up the data set into 631 chunks, with each chunk except for the last having 1000 rows.

num.row <- nrow(test_2014)
interval.begin <- seq(from = 1, to = num.row, by = 1000)
interval.end <- seq(from = 1000, to = num.row, by = 1000) %>% c(num.row)
num.chunks <- (num.row / 1000) %>% ceiling()

# The following lines can be used to procure the first 10000 rows of data to do preliminary work with.
# Unfortunately, as order of the rows matter, a random subset is not appropriate.
# interval.begin <- seq(from = 1, to = 10000, by = 1000)
# interval.end <- seq(from = 1000, to = 10000, by = 1000)
# num.chunks <- 10

cleaned.test <- list()
start.time <- proc.time()
for (i in 1:num.chunks) {
  cleaned.test[[i]] <- test_2014 %>%
    slice(interval.begin[i]:interval.end[i]) %>%
    CleanDataFrame() %>%
    RemoveBadData()
  if ((i %% 10) == 0) {
    time.elapsed <- proc.time() - start.time
    cat("Cleaning complete on row ", i, " of ", num.chunks, ".\n",
        "Estimated time remaining: ", (time.elapsed[3] * ((num.chunks - i) / i)) / 60, " minutes.\n", sep = "")
  }
}

cleaned.test.complete <- do.call("rbind", cleaned.test)

# Export cleaned data as a CSV file.
write.csv(cleaned.test.complete, file = "cleaned_test_2014.csv", row.names = FALSE)
