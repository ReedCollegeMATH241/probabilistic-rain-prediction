# Note that one needs to have Java Runtime Environment 8 installed for H2O to work.
library(h2o)
library(dplyr)
library(data.table)

# Import training data. Fix some type issues.
train.cleaned <- fread("cleaned_train_2013.csv", stringsAsFactors = FALSE) %>%
  tbl_df()
# The response variable is required to be integer as we want the deep learning algorithm to classify and not regress.
train.cleaned$Expected <- as.integer(train.cleaned$Expected)
train.cleaned$HydrometeorType <- as.integer(train.cleaned$HydrometeorType)

# Import test data.
test.cleaned <- fread("cleaned_test_2014.csv", stringsAsFactors = FALSE) %>%
  tbl_df()
test.cleaned$HydrometeorType <- as.integer(test.cleaned$HydrometeorType)

# Calculate means of each column for each observation ID and convert HydrometeorType back into a factor.
# I'm using a rather crude (and not necessarily 100% accurate) way of taking the mode of Hydrometeor type, sorry.
train <- train.cleaned %>%
  group_by(Id) %>%
  summarise_each(funs(mean(., na.rm = TRUE))) %>%
  mutate(HydrometeorType = as.factor(round(HydrometeorType)))
test <- test.cleaned %>%
  group_by(Id) %>%
  summarise_each(funs(mean(., na.rm = TRUE))) %>%
  mutate(HydrometeorType = as.factor(round(HydrometeorType)))

# The vast majority of the rain gauges read zero.
# In fact, I will claim that the probabilities of getting rainfall above 12mm is negligible for the scoring algorithm.
# Ignoring the upper range of rainfall will allow us to get better resolution for the lower range.
train <- filter(train, Expected < 13)

# Let's get rid of bad outliers from LogWaterVolume.
train <- filter(train, LogWaterVolume < -8)
train <- filter(train, LogWaterVolume > -15)

# Initialize a local H2O cluster using all available cores.
local.h2o <- h2o.init(nthreads = -1)

# Link the data sets to the H2O cluster.
train.h2o <- as.h2o(local.h2o, train, key = "train")
test.h2o <- as.h2o(local.h2o, test, key = "test")

# Split the training data set 70:30 for training and validation.
train.h2o.split <- h2o.splitFrame(train.h2o, ratios = 0.7, shuffle = TRUE)

# EDA suggests HydrometeorType, RR1, RR2, and LogWaterVolume as predictors.

# Train a deep neural network model.
model <- h2o.deeplearning(x = c(6, 8, 9, 17),                         # column numbers for predictors
                          y = 20,                                     # column number for response variable
                          data = train.h2o.split[[1]],                # training set
                          validation = train.h2o.split[[2]],          # validation set
                          activation = "TanhWithDropout",             # activation function
                          input_dropout_ratio = 0.2,
                          hidden_dropout_ratio = c(0.5, 0.5, 0.5),
                          balance_classes = FALSE,
                          hidden = c(50, 50, 50),
                          epochs = 200,                               # number of passes to carry out over training set
                          classification = TRUE)                      # for probability distribution instead of point estimate

# Use the model on the test set.
prediction <- h2o.predict(model, test.h2o) %>% as.data.frame()

# Kaggle has strict submission guidelines to follow.
PrepareForSubmission <- function(df) {
  
  num.col <- ncol(df)
  num.row <- nrow(df)
  
  # Create a vector of column names that's dictated by the submission format.
  submission.colnames <- vector()
  for (i in 1:70)
    submission.colnames[i] <- paste0("Predicted", i-1)
  submission.colnames <- c("Id", submission.colnames)
  
  for (j in 3:num.col) {
    df[, j] <- df[, (j - 1)] + df[, j]
  }
  
  col.of.ones <- rep(1, num.row)
  for(j in (num.col + 1):71) {
    df <- cbind(df, col.of.ones)
  }
  
  colnames(df) <- submission.colnames
  df$Id <- test$Id
  
  return(df)
}

prediction.prepared <- PrepareForSubmission(prediction)

# Export predictionas a CSV file.
write.csv(prediction.prepared, file = "prediction_v2.csv", row.names = FALSE, quote = FALSE)

