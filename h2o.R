# Note that one needs to have Java Runtime Environment 8 installed for H2O to work.
library(h2o)
library(dplyr)
library(data.table)

# Import training data. Fix some type issues.
train <- fread("cleaned_train_2013.csv", stringsAsFactors = FALSE) %>%
  tbl_df()
train$HydrometeorType <- as.factor(train$HydrometeorType)
# The response variable is required to be integer as we want the deep learning algorithm to classify and not regress.
train$Expected <- as.integer(train$Expected)

# Import test data.
test <- fread("cleaned_test_2014.csv", stringsAsFactors = FALSE) %>%
  tbl_df()
test$HydrometeorType <- as.factor(test$HydrometeorType)

# Calculate means of each column for each observation ID.
train <- train %>%
  group_by(Id) %>%
  summarise_each(funs(mean(., na.rm = TRUE)))
test <- test %>%
  group_by(Id) %>%
  summarise_each(funs(mean(., na.rm = TRUE)))

# Initialize a local H2O cluster using all available cores.
local.h2o <- h2o.init(nthreads = -1)

# Link the data sets to the H2O cluster.
train.h2o <- as.h2o(local.h2o, train, key = "train")
test.h2o <- as.h2o(local.h2o, test, key = "test")

# Split the training data set 70:30 for training and validation.
train.h2o.split <- h2o.splitFrame(train.h2o, ratios = 0.7, shuffle = TRUE)

# Train a deep neural network model.
model <- h2o.deeplearning(x = 2:19,                                   # column numbers for predictors
                          y = 20,                                     # column number for response variable
                          data = train.h2o.split[[1]],                # training set
                          validation = train.h2o.split[[2]],          # validation set
                          activation = "TanhWithDropout",             # activation function
                          input_dropout_ratio = 0.2,
                          hidden_dropout_ratio = c(0.5, 0.5, 0.5),
                          balance_classes = FALSE,
                          hidden = c(50, 50, 50),
                          epochs = 100,                               # number of passes to carry out over training set
                          classification = TRUE)                      # for probability distribution instead of point estimate

prediction <- h2o.predict(model, test.h2o) %>% as.data.frame()
