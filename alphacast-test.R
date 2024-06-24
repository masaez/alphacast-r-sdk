source("alphacast.R")

csv <- read.csv("test.csv")

# Create an Alphacast account, and get your API key from the settings page
api_key <- "my key"

datasets <- read_all_datasets(api_key)

stats <- read_dataset_by_name(api_key, "Comparative portfolio stats")

dataset_id <- 41975
upload_data_from_df(api_key, dataset_id, df = csv,
                    accept_new_columns = TRUE,
                    date_column_name = "Date",
                    date_format = "%Y-%m-%d",
                    entities_column_names = list("country",
        "Alimentos y bebidas no alcohólicas")) # nolint: indentation_linter.
