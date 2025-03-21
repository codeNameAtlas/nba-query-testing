# NBA Natural Language to SQL Testing

This project tests Claude's ability to convert natural language questions about NBA data into SQL queries that match the expected results from ground truth queries.

## Project Structure

- `nba-query-notebook.py` - Python script version that can be run directly
- `nl_to_sql_notebook.ipynb` - Jupyter notebook version for interactive testing
- `nba_database.sqlite` - SQLite database containing NBA data
- `ground_truth_data.json` - Test cases with natural language questions and expected SQL queries
- `.env` - Environment file for API key
- `requirements.txt` - Dependencies list

## Setup Instructions

### 1. Clone/Download the Project

Download all files to your local machine.

### 2. Install Requirements

```bash
# Create a virtual environment (recommended)
python -m venv nba-env

# Activate the virtual environment
# On Windows:
nba-env\Scripts\activate
# On Mac/Linux:
source nba-env/bin/activate

# Install requirements
pip install -r requirements.txt
```

### 3. Set Up API Key

1. Create an account on [Anthropic](https://www.anthropic.com/) if you don't have one
2. Get your API key from the Anthropic dashboard
3. Create a `.env` file in the project directory with the following content:
   ```
   ANTHROPIC_API_KEY=your_api_key_here
   ```

### 4. Running the Project

#### Option 1: Run the Python Script

To run the Python script version which will automatically test 5 random examples:

```bash
python nba-query-notebook.py
```

This will execute the entire testing process and show the results.

#### Option 2: Use the Jupyter Notebook

For interactive exploration and testing:

```bash
jupyter notebook nl_to_sql_notebook.ipynb
```

The notebook lets you:
- Run cells individually to see intermediate results
- Modify the prompt or testing parameters
- Test specific examples instead of random ones
- Analyze results in more detail

## How It Works

The project uses Claude's Opus model to convert natural language questions about NBA data into SQL queries. The key innovation is the feedback mechanism:

1. A natural language question is presented to Claude along with the database schema
2. The expected SQL (from ground truth) is also provided as feedback
3. Claude analyzes both its generated SQL and the expected SQL
4. The result is a more accurate SQL query that matches the expected output

This approach achieves high accuracy in generating correct SQL queries.

## Customization

To modify the testing process:
- Adjust the `num_examples` parameter in `run_example_tests()` to test more or fewer examples
- Set `use_feedback=False` to test without the feedback mechanism
- Modify the prompt in `get_sql_from_claude_with_feedback()` to experiment with different prompting techniques

## Dependencies

- anthropic
- python-dotenv
- jupyter (for notebook option)
- sqlite3 (standard library)
