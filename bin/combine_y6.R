library(dplyr)
library(tidyr)
library(tidyverse)
library(data.table)
library(tools)

# Set path
setwd("/export/projects4/jwalsh_banks")

# Set globals
START_YEAR <- 1995

years <- START_YEAR:1995

REQUIRED <- c("Bank Name","table presence","Bank_PDF-Name","Year",
              "source_file","source_year")#,"a1","b1","c1","d1","e1","f1","g1")

# columns that must be present IN THE CSV ITSELF (exclude the two provenance cols you add later)
REQUIRED_FILE <- setdiff(REQUIRED, c("source_file","source_year"))

# Full file
FULL_SEC <-c("Bank Name","table presence","Bank_PDF-Name","Year",
                "source_file","source_year","a1","b1","c1","d1","e1","f1","g1")

FULL_INS <- c("Bank Name","table presence","Bank_PDF-Name","Year",
                   "source_file","source_year",
              "a2","b2","b3","b4","b5","b6","b7",
              "b8", "b9", "b10", "b11","b9_full_voting_shares_text"
              )

###############################################################################
# Look for bad files that justify using read_csv vs. fread
# Also track the files that have extra columns in it: These we will probably
# Want to repeat the work for manually.
###############################################################################
validate_dir <- function(base_dir, years, required_file=REQUIRED_FILE) {
  dirs  <- file.path(base_dir, years)
  dirs  <- dirs[dir.exists(dirs)]
  files <- unlist(lapply(dirs, function(d) list.files(d, pattern="\\.csv$", full.names=TRUE)), use.names=FALSE)
  if (!length(files)) return(list(log=data.table(), bad_files=data.table()))
  
  logs <- lapply(files, function(f) {
    y <- as.integer(basename(dirname(f)))
    dt <- tryCatch(fread(f, nrows=0, colClasses="character"),
                   error=function(e) NULL)
    if (is.null(dt)) {
      return(data.table(source_file=basename(f), source_year=y,
                        status="read_error", missing_cols=NA_character_,
                        duplicate_cols=NA_character_, extra_cols=NA_character_))
    }
  
    
    # check for issues AFTER aliasing
    missing   <- setdiff(required_file, names(dt))
    dups      <- unique(names(dt)[duplicated(names(dt))])
    extras    <- setdiff(names(dt), required_file)
    
    data.table(
      source_file  = basename(f),
      source_year  = y,
      status       = ifelse(length(missing)==0 && length(dups)==0, "ok", "bad"),
      missing_cols = if (length(missing)) paste(missing, collapse="|") else "",
      duplicate_cols = if (length(dups)) paste(dups, collapse="|") else "",
      extra_cols   = if (length(extras)) paste(extras, collapse="|") else ""
    )
  })
  
  log_dt    <- rbindlist(logs, use.names=TRUE, fill=TRUE)
  bad_files <- log_dt[status!="ok", .(source_file, source_year, missing_cols, duplicate_cols)]
  list(log=log_dt, bad_files=bad_files)
}

# Run it
sec_val <- validate_dir("fed-data/Gemini/csv_testing/securities", years)
ins_val <- validate_dir("fed-data/Gemini/csv_testing/insiders",   years)

# Files that don't adhere to the naming schema:
sec_val$bad_files
ins_val$bad_files

sec_cols <- sec_val$log
ins_cols <- ins_val$log

# Save test as a file for data quality later.
fwrite(sec_cols, "output/securities_colissues.csv")
fwrite(ins_cols, "output/insiders_colissues.csv")

###############################################################################
# Extract the securities tables using read_csv
###############################################################################

read_yeared_csvs <- function(base_dir, years, fread=TRUE) {
  dt_list <- lapply(years, function(y) {
    dir_y <- file.path(base_dir, y)
    if (!dir.exists(dir_y)) return(NULL)
    
    files <- list.files(dir_y, pattern = "\\.csv$", full.names = TRUE)
    if (length(files) == 0) return(NULL)
    
    if (fread==TRUE) {
    lapply(files, function(f) {
      # Read as character to avoid type conflicts; read_csv handles multiline quoted fields well
      tryCatch({
        df <- fread(
          f#,
         # col_types = cols(.default = col_character()),
         # show_col_types = FALSE
        )
        print(length(df))
        dt <- as.data.table(df)
        dt[, `:=`(source_file = basename(f), source_year = y)]
        dt
      }, error = function(e) {
        warning(sprintf("Skipping %s: %s", f, e$message))
        NULL
      })
    })
    }  else {
      lapply(files, function(f) {
        # Read as character to avoid type conflicts; read_csv handles multiline quoted fields well
        tryCatch({
          df <- read_csv(
            f,
            col_types = cols(.default = col_character()),
            show_col_types = FALSE
          )
          print(length(df))
          dt <- as.data.table(df)
          dt[, `:=`(source_file = basename(f), source_year = y)]
          dt
        }, error = function(e) {
          warning(sprintf("Skipping %s: %s", f, e$message))
          NULL
        })
      })
    }
    
  })
  
  # Flatten one level only; keep as a list of tables
  dt_list <- unlist(dt_list, recursive = FALSE)
  dt_list <- Filter(Negate(is.null), dt_list)
  if (length(dt_list) == 0) return(data.table())
  
  rbindlist(dt_list, use.names = TRUE, fill = TRUE)
}

securities <- read_yeared_csvs("fed-data/Gemini/csv_testing/securities", years, fread=TRUE) 
insiders   <- read_yeared_csvs("fed-data/Gemini/csv_testing/insiders",   years, fread=TRUE)

stop("here")
insiders <- insiders %>% select(FULL_INS)

print("save combined files")
fwrite(securities, "output/securities_raw.csv")
fwrite(insiders, "output/insiders_raw.csv")


stop("stop here")

###############################################################################
# Extract the Federal Reserve tables
###############################################################################

districts <- c("Atlanta", "Cleveland", "Dallas", "Minneapolis", "Richmond")

print("district securities")
securities_d <- read_yeared_csvs("fed-data/Gemini/csv_testing/securities", districts)

print("district insiders")
insiders_d  <- read_yeared_csvs("fed-data/Gemini/csv_testing/insiders",   districts)

# Save the combined files
print("save combined files")
fwrite(securities_d, "output/securities_raw_d.csv")
fwrite(insiders_d, "output/insiders_raw_d.csv")

