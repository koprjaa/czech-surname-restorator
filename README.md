# CSV Name Correction Tool

A Python tool for correcting Czech surnames in CSV files by matching them against a reference database and applying proper diacritics. The tool uses parallel processing with Ray for efficient handling of large datasets.

## Features

- **Diacritics Correction**: Automatically adds missing diacritics to Czech surnames
- **Fuzzy Matching**: Uses advanced fuzzy string matching for name correction
- **Parallel Processing**: Leverages Ray for distributed processing of large datasets
- **Chunked Processing**: Handles large CSV files efficiently by processing them in chunks
- **Comprehensive Logging**: Detailed logging with both file and console output
- **Index-based Lookup**: Fast surname matching using pre-built indexes

## Requirements

- Python 3.8+
- See `requirements.txt` for specific package versions

## Installation

1. Clone or download this repository
2. Install required dependencies:

```bash
pip install -r requirements.txt
```

## Usage

### Basic Usage

1. Prepare your input CSV file (`FO_ALL.csv`) with a column named `Person_Name`
2. Ensure your reference CSV file (`prijmeni.csv`) contains surnames in the first column (`column1`)
3. Run the script:

```bash
python main.py
```

### Input Files

- **Input CSV** (`FO_ALL.csv`): Your data file containing names to be corrected
  - Must have a column named `Person_Name` with full names
  - Names should be in format "FirstName LastName"

- **Reference CSV** (`prijmeni.csv`): Database of correct Czech surnames
  - Must have surnames in the first column (`column1`)
  - Should contain properly formatted Czech surnames with diacritics

### Output

The tool generates:
- **Corrected CSV** (`FO_ALL_opravene.csv`): Input data with corrected surnames
  - Original columns preserved
  - New columns added:
    - `krestni_jmeno`: Extracted first name
    - `prijmeni`: Extracted surname
    - `opravene_prijmeni`: Corrected surname with proper diacritics
    - `opravene_jmeno`: Full corrected name (first name + corrected surname)
- **Log file** (`name_correction.log`): Detailed processing log

## Configuration

You can modify these constants in `main.py`:

```python
REFERENCE_CSV = "prijmeni.csv"    # Reference surnames file
INPUT_CSV = "FO_ALL.csv"          # Input data file
OUTPUT_CSV = "FO_ALL_opravene.csv" # Output file
CHUNK_SIZE = 10000                # Records per chunk
BATCH_SIZE = 1000                 # Records per Ray batch
```

## How It Works

1. **Index Creation**: Builds an efficient index from reference surnames based on grapheme count and normalized (diacritics-removed) names
2. **Chunked Processing**: Processes input CSV in configurable chunks to handle large files
3. **Parallel Correction**: Uses Ray to distribute surname correction across multiple CPU cores
4. **Multi-level Matching**:
   - Direct index lookup for exact matches
   - Fuzzy matching within index groups for similar names
   - Fallback fuzzy search across entire reference database
5. **Diacritics Preservation**: Intelligently combines diacritics from original and corrected names

## Performance

- Optimized for large datasets (tested with 100k+ records)
- Parallel processing scales with available CPU cores
- Memory-efficient chunked processing
- Fast index-based lookups reduce computation time

## Logging

The tool provides comprehensive logging:
- Processing progress and statistics
- Error handling and warnings
- Performance metrics
- Both file (`name_correction.log`) and console output

## Error Handling

- Gracefully handles non-string inputs
- Continues processing even if individual names fail
- Comprehensive logging for debugging
- Automatic cleanup of old output files

## Dependencies

- **pandas**: Data manipulation and CSV handling
- **ray**: Distributed computing framework
- **regex**: Advanced regular expressions for Unicode handling
- **rapidfuzz**: Fast fuzzy string matching

## License

This project is provided as-is for educational and research purposes.

## Contributing

Feel free to submit issues, feature requests, or pull requests to improve the tool.
