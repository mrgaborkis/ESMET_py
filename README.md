# ESMETL Technical Documentation
Enterprise SQL DBA friendly Configuration via JSON no programming

### SQL-Compatible Metadata-Driven ETL Framework

**ESMETL** is a Python-based ingestion engine designed to decouple business logic from source code.
By utilizing a **Metadata Markup Language (MML)** via JSON,
the framework allows Data Engineers and DBAs to define complex "daisy-chained" transformations using standard T-SQL logic.

## 1. System Architecture
The system operates on a **"Generic Engine"** model. Instead of writing custom code for every data source, you provide a configuration file that instructs the engine on how to handle the specific schema.

* **Controller** (`meatada_transform_etl.py`): Handles file I/O and reads environment settings.
* **Engine** (`ingestor_engine.py`): The logical processor that executes the transformation stack.
* **Metadata** (`config.json`): The instruction set defining mappings and quality gates.

## 2. Global Settings
The `settings` block in `config.json` controls the I/O behavior of the ingestion process.

| Parameter          | Example | Description                                      |
|:-------------------|:--------|:-------------------------------------------------|
| `input_delimiter`  | `"\t"`  | The separator for the source file (Tab, Comma, Pipe). |
| `output_delimiter` | `"|"`   | The separator for the destination file.          |

## 3. Transformation Function Library
The engine supports **Daisy Chaining**, allowing you to stack multiple functions in a single array. They execute sequentially from index `0` to `n`.

### A. String & Formatting Functions
| Function       | Syntax          | SQL Equivalent | Description                        |
|:---------------|:----------------|:---------------|:-----------------------------------|
| **TRIM** | `TRIM`          | `TRIM()`       | Removes leading/trailing whitespace. |
| **UPPER** | `UPPER`         | `UPPER()`      | Converts to all caps.              |
| **LOWER** | `LOWER`         | `LOWER()`      | Converts to lowercase.             |
| **CAPITALIZE** | `CAPITALIZE`    | `INITCAP()`    | Capitalizes first letter only.     |
| **LEFT** | `LEFT:n`        | `LEFT(col, n)` | Returns the first n characters.    |
| **RIGHT** | `RIGHT:n`       | `RIGHT(col, n)`| Returns the last n characters.     |
| **LPAD** | `LPAD:len|char` | `LPAD()`       | Pads left to len using char.       |
| **CLEAN_PHONE**| `CLEAN_PHONE`   | N/A            | Strips all non-numeric characters. |

### B. Logic & Math Functions
| Function     | Syntax        | SQL Equivalent | Description                        |
|:-------------|:--------------|:---------------|:-----------------------------------|
| **COALESCE** | `COALESCE:val`| `COALESCE()`   | Replaces NULLs/NaNs with val.      |
| **REPLACE** | `REPLACE:a|b` | `REPLACE()`    | Swaps string a for string b.       |
| **CAST** | `CAST:type`   | `CAST(x AS y)` | Forces type INT or FLOAT.          |
| **DIVIDE** | `DIVIDE:n`    | `ncol / n`     | Divides numeric value by n.        |

### C. Date & Quality Functions
| Function          | Syntax          | Description                                      |
|:------------------|:----------------|:-------------------------------------------------|
| **DT_TO_YYMMDD** | `DT_TO_YYMMDD`  | Converts any readable date to YYMMDD format.     |
| **VALIDATE** | `VALIDATE:NUM`  | Logs an error if any row contains non-numeric data.|
| **VALIDATE** | `VALIDATE:NULL` | Logs an error if any row contains empty values.  |

## 4. Advanced Usage: Daisy Chaining
Daisy chaining allows you to transform "dirty" data into production-ready values by stacking operations.

Scenario: A currency column raw_total contains "$ 1,250.00 ". You need a clean float.

**Metadata Logic:**
- TRIM: Removes the trailing space.
- REPLACE: Removes the $.
- REPLACE: Removes the ,.
- VALIDATE: Ensures the remaining string "1250.00" is a valid number.
- CAST: Converts the string to a floating-point number.

**Metadata Configuration:**
```json
"transform": ["TRIM", "REPLACE:$|", "REPLACE:,|", "VALIDATE:NUMERIC", "CAST:FLOAT"]


## 5. Error Handling & Quality Reports
Unlike traditional scripts that crash on bad data, ESMETL utilizes a Non-Breaking Quality Gate. 
If a VALIDATE tag fails:

- The engine completes the run.
- A Data Quality Report is generated in the console.
- The report identifies specific column failures (e.g., Field 'emp_id': NULL/Blank values found) while still processing the rest of the batch.

##6. Implementation Example

Input File (test_input.csv)
---------------------------
emp_id	salary
101	$95,000
ERR	$100,000

Config File (config.json)
-------------------------
{
  "settings": { 
    "input_delimiter": "\t", 
    "output_delimiter": "|" 
  },
  "mappings": [
    { 
      "source": "emp_id", 
      "alias": "ID", 
      "transform": ["VALIDATE:NUMERIC", "LPAD:5|0"] 
    },
    {
      "source": "salary",
      "alias": "Salary",
      "transform": ["REPLACE:$|", "REPLACE:,|", "CAST:FLOAT"]
    }
  ]
}

Output File (final_output.txt)
------------------------------
ID|Salary
00101|95000.0

##7. Developer Notes (Python Examples)
To extend the engine, add a new case to the _transform_logic method in ingestor_engine.py.

Standard Python Extension Syntax:

# ingestor_engine.py excerpt
def _transform_logic(self, df, source_col, action):
    if action == "TRIM":
        df[source_col] = df[source_col].astype(str).str.strip()
    
    elif action.startswith("REPLACE:"):
        # Syntax: REPLACE:old|new
        parts = action.split(":")[1].split("|")
        old_val, new_val = parts[0], parts[1]
        df[source_col] = df[source_col].astype(str).str.replace(old_val, new_val, regex=False)

    elif action.startswith("LPAD:"):
        # Syntax: LPAD:length|char
        parts = action.split(":")[1].split("|")
        length, char = int(parts[0]), parts[1]
        df[source_col] = df[source_col].astype(str).str.rjust(length, char)

    elif action == "YOUR_TAG":
        # Add custom logic here
        df[source_col] = df[source_col].apply(lambda x: custom_func(x))

    return df
