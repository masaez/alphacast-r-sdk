library(httr)
library(jsonlite)
library(readr)

base_url <- "https://api.alphacast.io"

# Utility function to handle API requests
api_request <- function(api_key, method, path, body = NULL) {
  url <- paste0(base_url, path)
  response <- VERB(method, url, authenticate(api_key, ""), body = body,
                   encode = "json")

  if (http_status(response)$category != "Success") {
    rjson <- tryCatch(
      {
        content(response, "parsed")
      },
      error = function(e) {
        stop("API failed with status code ", status_code(response))
      }
    )

    if (!is.null(rjson$message)) {
      stop(status_code(response), ": ", rjson$message)
    }
  }

  return(content(response, "parsed"))
}

# Datasets functions
read_all_datasets <- function(api_key) {
  api_request(api_key, "GET", "/datasets")
}

read_dataset_by_name <- function(api_key, dataset_name, repo_id = NULL) {
  datasets <- read_all_datasets(api_key)
  for (dataset in datasets) {
    if (dataset$name == dataset_name &&
          (is.null(repo_id) || dataset$repositoryId == repo_id)) {
      return(dataset)
    }
  }
  return(NULL)
}

create_dataset <- function(api_key, dataset_name, repo_id,
                           description = "") {
  previous_dataset <- read_dataset_by_name(api_key, dataset_name, repo_id)
  if (!is.null(previous_dataset)) {
    stop("Dataset already exists: ", previous_dataset$id)
  }

  body <- list(
    name = dataset_name,
    repositoryId = repo_id,
    description = description
  )
  api_request(api_key, "POST", "/datasets", body)
}

get_dataset_metadata <- function(api_key, dataset_id) {
  api_request(api_key, "GET", paste0("/datasets/", dataset_id))
}

get_dataset_column_definitions <- function(api_key, dataset_id) {
  response <- api_request(api_key, "GET",
                          paste0("/datasets/", dataset_id, "/columns"))
  return(response$columnDefinitions)
}

download_dataset_data <- function(api_key, dataset_id, format = "csv",
                                  start_date = NULL, end_date = NULL,
                                  filter_variables = list(),
                                  filter_entities = list()) {
  date_column_name <- "Date"
  all_filters <- list()
  entity_query_filter <- ""

  if (length(filter_entities) > 0) {
    entity_query_params <- sapply(names(filter_entities), function(entity) {
      paste(sapply(filter_entities[[entity]], function(value) {
        paste0(entity, " eq '", value, "'")
      }), collapse = " or ")
    })
    entity_query_filter <- paste(entity_query_params, collapse = " and ")
  }

  date_filters <- c()
  if (!is.null(start_date) || !is.null(end_date)) {
    columns <- get_dataset_column_definitions(api_key, dataset_id)
    date_column_name <- columns[sapply(columns,
                                  function(c) c$dataType == "Date"
                                ), "sourceName"]

    if (!is.null(start_date)) {
      date_filters <- c(date_filters, paste0("'", date_column_name,
                                             "' ge ",
                                             format(start_date, "%Y-%m-%d")))
    }

    if (!is.null(end_date)) {
      date_filters <- c(date_filters, paste0("'", date_column_name,
                                             "' le ",
                                             format(end_date, "%Y-%m-%d")))
    }
  }

  date_filter <- paste(date_filters, collapse = " and ")
  date_and_entity_filter <- paste(c(date_filter, entity_query_filter),
                                  collapse = " and ")

  if (nchar(date_and_entity_filter) > 0) {
    all_filters[["$filter"]] <- date_and_entity_filter
  }

  if (length(filter_variables) > 0) {
    all_filters[["$select"]] <- paste(filter_variables, collapse = ",")
  }

  query_string <- URLencode(http_build_query(all_filters))

  if (nchar(query_string) > 0) {
    query_string <- paste0("&", query_string)
  }

  return_format <- format
  if (format == "dataframe") return_format <- "csv"

  response <- api_request(api_key, "GET",
                          paste0("/datasets/", dataset_id, "/data?",
                                 query_string, "&$format=", return_format))

  if (format == "json") {
    return(response)
  } else if (format == "dataframe") {
    return(read_csv(content(response, "text")))
  } else {
    return(content(response, "text"))
  }
}

upload_data_from_df <- function(api_key, dataset_id, df,
                                delete_missing_from_db = FALSE,
                                on_conflict_update_db = FALSE,
                                upload_index = TRUE,
                                date_column_name = NULL,
                                date_format = NULL,
                                entities_column_names = list(),
                                string_column_names = list(),
                                accept_new_columns = NULL) {
  if (nrow(df) == 0) {
    stop("Dataframe is empty.")
  }

  csv <- format_csv(df, col_names = TRUE)
  upload_data_from_csv(api_key, dataset_id, csv, delete_missing_from_db,
                       on_conflict_update_db, date_column_name, date_format,
                       entities_column_names, string_column_names,
                       accept_new_columns)
}

get_initializer <- function(date_column_name,  # nolint: cyclocomp_linter.
                            date_format,
                            entities_column_names,
                            string_column_names) {

  if (is.null(date_column_name) && is.null(date_format) &&
        length(entities_column_names) == 0 &&
        length(string_column_names) == 0) {
    return(NULL)
  }

  manifest <- list()
  if (!is.null(date_column_name) && !is.null(date_format)) {
    date_descriptor <- list(sourceName = date_column_name,
                            isEntity = TRUE,
                            dataType = "Date",
                            dateFormat = date_format)
    manifest <- c(manifest, list(date_descriptor))
  }
  if (length(entities_column_names) > 0) {
    manifest <- c(manifest, lapply(entities_column_names, function(c) {
      list(sourceName = c, isEntity = TRUE, dataType = "String")
    }))
  }
  if (length(string_column_names) > 0) {
    manifest <- c(manifest, lapply(string_column_names, function(c) {
      list(sourceName = c, isEntity = FALSE, dataType = "String")
    }))
  }

  return(manifest)
}


upload_data_from_csv <- function(api_key, dataset_id, csv,
                                 delete_missing_from_db = FALSE,
                                 on_conflict_update_db = FALSE,
                                 date_column_name = NULL,
                                 date_format = NULL,
                                 entities_column_names = list(),
                                 string_column_names = list(),
                                 accept_new_columns = NULL) {

  initializer <- get_initializer(date_column_name, date_format,
                                 entities_column_names, string_column_names)

  url <- paste0(base_url, "/datasets/", dataset_id,
                "/data?deleteMissingFromDB=", delete_missing_from_db,
                "&onConflictUpdateDB=", on_conflict_update_db)
  if (!is.null(accept_new_columns)) {
    url <- paste0(url, "&acceptNewColumns=", accept_new_columns)
  }

  temp_file <- tempfile()
  writeLines(csv, temp_file)

  initializer_string <- toJSON(initializer, auto_unbox = TRUE)
  print(initializer_string)

  files <- list(data = upload_file(temp_file, type = "text/csv"),
                manifest = initializer_string)

  print(files)

  response <- PUT(url, body = files,
                  authenticate(api_key, ""), encode = "multipart")
  content(response, "text")
}

# Repositories functions
read_all_repositories <- function(api_key) {
  api_request(api_key, "GET", "/repositories")
}

read_repository_by_id <- function(api_key, repository_id) {
  api_request(api_key, "GET", paste0("/repositories/", repository_id))
}

read_repository_by_name <- function(api_key, repo_name) {
  repositories <- read_all_repositories(api_key)
  for (repo in repositories) {
    if (repo$name == repo_name) {
      return(repo)
    }
  }
  return(NULL)
}

create_repository <- function(api_key, repo_name, repo_description = NULL,
                              privacy = "Private", slug = NULL) {
  if (is.null(slug)) {
    slug <- tolower(gsub(" ", "-", repo_name))
  }
  if (is.null(repo_description)) {
    repo_description <- repo_name
  }

  existing_repo <- read_repository_by_name(api_key, repo_name)
  if (!is.null(existing_repo)) {
    stop("Repository already exists: ", existing_repo$id)
  }

  body <- list(
    name = repo_name,
    description = repo_description,
    privacy = privacy,
    slug = slug
  )

  api_request(api_key, "POST", "/repositories", body)
}

delete_repository <- function(api_key, repository_id) {
  api_request(api_key, "DELETE", paste0("/repositories/", repository_id))
}
