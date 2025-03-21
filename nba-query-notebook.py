# NBA Natural Language to SQL Testing Notebook
# ============================================
#
# This notebook tests Claude's ability to convert natural language queries about NBA data
# into SQL queries that match the expected results from ground truth queries.

# --- Setup and Imports ---

import os
import json
import random
import sqlite3
import re
import anthropic
from dotenv import load_dotenv
from IPython.display import Markdown, display

# Load environment variables (API key)
load_dotenv()

# Initialize Anthropic client
client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))

# --- Database Connection ---

# Update this path to your SQLite database file
DB_PATH = "nba_database.sqlite"

def connect_to_db():
    """Connect to the SQLite database and return connection object."""
    try:
        conn = sqlite3.connect(DB_PATH)
        print("✅ Connected to database successfully")
        return conn
    except sqlite3.Error as e:
        print(f"❌ Error connecting to database: {e}")
        return None

# --- Ground Truth Data Loading ---

def load_ground_truth_data(json_path="ground_truth_data.json"):
    """Load ground truth data from JSON file."""
    try:
        with open(json_path, 'r') as f:
            data = json.load(f)
        print(f"✅ Loaded {len(data)} ground truth examples")
        return data
    except Exception as e:
        print(f"❌ Error loading ground truth data: {e}")
        return []

# --- Query Execution Functions ---

def execute_sql_query(conn, query):
    """Execute SQL query and return results as a list of tuples."""
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        
        # Get column names
        column_names = [description[0] for description in cursor.description]
        
        # Fetch all rows
        rows = cursor.fetchall()
        
        return {
            'column_names': column_names,
            'rows': rows
        }
    except Exception as e:
        print(f"❌ Error executing query: {e}")
        print(f"Query was: {query}")
        return None

def compare_query_results(ground_truth_results, generated_results):
    """
    Compare results from ground truth and generated queries.
    Returns True if results match, False otherwise.
    Focus on content rather than structure.
    """
    if ground_truth_results is None or generated_results is None:
        return False
    
    # If either result set is empty, both should be empty
    if len(ground_truth_results['rows']) == 0:
        return len(generated_results['rows']) == 0
    
    # Handle single row, single value results (most common case)
    if len(ground_truth_results['rows']) == 1 and len(ground_truth_results['column_names']) == 1:
        gt_val = ground_truth_results['rows'][0][0]
        
        # Generated result might have multiple columns, but we just check first column of first row
        if len(generated_results['rows']) > 0 and len(generated_results['rows'][0]) > 0:
            gen_val = generated_results['rows'][0][0]
            
            # For numeric values, check with tolerance
            if isinstance(gt_val, (int, float)) and isinstance(gen_val, (int, float)):
                return abs(gt_val - gen_val) < 0.01
            # For strings or other types, check exact match
            return gt_val == gen_val
        return False
    
    # For multi-row results, convert to sets of tuples for comparison (ignores order)
    # Only compare the first column if ground truth has just one column
    if len(ground_truth_results['column_names']) == 1:
        gt_set = set(row[0] for row in ground_truth_results['rows'])
        
        # Generated result might have multiple columns, but we just check first column
        gen_set = set(row[0] for row in generated_results['rows'] if len(row) > 0)
        
        # If we're looking at a sample (first few rows only), just check these are in generated results
        if len(ground_truth_results['rows']) <= 5:
            return all(val in gen_set for val in gt_set)
        
        # Otherwise, check the sets are the same
        return gt_set == gen_set
    
    # If multiple columns, convert to sets of tuples
    gt_set = set(tuple(row) for row in ground_truth_results['rows'])
    gen_set = set(tuple(row) for row in generated_results['rows'])
    
    # If we're looking at a sample (first few rows only), check these are in generated results
    if len(ground_truth_results['rows']) <= 5:
        return all(row in gen_set for row in gt_set)
    
    # Otherwise compare the full sets
    return gt_set == gen_set

# --- Claude API Interaction with Feedback ---

def get_sql_from_claude_with_feedback(question, expected_sql=None):
    """Get SQL query from Claude for the given natural language question with feedback."""
    feedback_mode = expected_sql is not None
    
    # Build the prompt
    prompt_text = """You are an AI assistant tasked with converting natural language queries about the NBA into SQL queries. You will be provided with a database schema to help you understand the structure of the data and formulate correct SQL queries.

<schema>
Database structure:
   - game: game_id, team_id_home, team_name_home, team_id_away, team_name_away, pts_home, pts_away, season_type, fg3m_home, fg3m_away, fg3a_home, fg3a_away, ftm_home, ftm_away, fta_home, fta_away, ast_home, ast_away, reb_home, reb_away, oreb_home, oreb_away, dreb_home, dreb_away, blk_home, blk_away, stl_home, stl_away, tov_home, tov_away, pf_home, pf_away
   - team: id, full_name, abbreviation, nickname, city, state, year_founded
   - player: id, full_name, first_name, last_name, is_active
   - common_player_info: person_id, first_name, last_name, position, height, weight, country, jersey, team_id, season_exp, school
   - game_info: game_id, game_date, attendance, game_time
   - line_score: game_id, team_id_home, team_id_away, pts_ot1_home, pts_ot1_away, pts_home, pts_away
   - draft_history: person_id, player_name, season, round_number, overall_pick, organization, organization_type
   - other_stats: game_id, team_id_home, team_id_away, lead_changes, pts_paint_home, pts_paint_away, pts_fb_home, pts_fb_away
   - inactive_players: player_id, first_name, last_name, team_id, game_id
   - team_details: team_id, arena, arenacapacity
   - team_history: team_id, city, nickname, year_founded, year_active_till
</schema>

<key_relationships>
- game.team_id_home → team.id
- game.team_id_away → team.id
- common_player_info.person_id → player.id
- common_player_info.team_id → team.id
- game_info.game_id → game.game_id
- line_score.game_id → game.game_id
- other_stats.game_id → game.game_id
- inactive_players.team_id → team.id
- inactive_players.game_id → game.game_id
- team_details.team_id → team.id
- team_history.team_id → team.id
</key_relationships>

<query>{0}</query>

Please analyze the query and think through how to convert it into SQL. Consider the following:
1. Which table(s) in the schema are relevant to this query?
2. What columns need to be selected? Do not create new columns.
3. Are any aggregations or groupings required?
4. Are there any conditions that need to be applied (WHERE clause)?
5. Is there a limit on the number of results to return?

Before answering, here are some examples. You can see there is a "natural language" field, and a "sql" field.

<example>
[
  {{
    "natural_language": "How many teams are currently in the NBA?",
    "sql": "SELECT COUNT(*) as team_count FROM team LIMIT 1",
    "type": "counting"
  }},
  {{
    "natural_language": "List all teams from Texas.",
    "sql": "SELECT full_name FROM team WHERE state = 'Texas'",
    "type": "filtering"
  }},
  {{
    "natural_language": "What's the lowest scoring game?",
    "sql": "SELECT g.pts_home + g.pts_away as total_points FROM game g ORDER BY total_points ASC LIMIT 1",
    "type": "ranking"
  }},
  {{
    "natural_language": "Which team has the most away games?",
    "sql": "SELECT t.full_name FROM game g JOIN team t ON g.team_id_away = t.id GROUP BY t.id, t.full_name ORDER BY COUNT(*) DESC LIMIT 1",
    "type": "ranking"
  }},
  {{
    "natural_language": "List all players from France.",
    "sql": "SELECT first_name, last_name FROM common_player_info WHERE country = 'France'",
    "type": "filtering"
  }},
  {{
    "natural_language": "What's the most common jersey number above 10?",
    "sql": "SELECT jersey FROM common_player_info WHERE CAST(jersey AS INTEGER) > 10 GROUP BY jersey ORDER BY COUNT(*) DESC LIMIT 1",
    "type": "ranking"
  }},
  {{
    "natural_language": "What's the average weight of NBA players?",
    "sql": "SELECT ROUND(AVG(CAST(weight AS FLOAT)), 2) as avg_weight FROM common_player_info WHERE weight != '' LIMIT 1",
    "type": "aggregation"
  }},
  {{
    "natural_language": "Which team has the oldest arena?",
    "sql": "SELECT t.full_name FROM team t JOIN team_details td ON t.id = td.team_id ORDER BY td.arena ASC LIMIT 1",
    "type": "detail"
  }},
  {{
    "natural_language": "List all second-year players.",
    "sql": "SELECT first_name, last_name FROM common_player_info WHERE season_exp = 1",
    "type": "filtering"
  }},
  {{
    "natural_language": "What's the most points scored by the away team?",
    "sql": "SELECT pts_away FROM game ORDER BY pts_away DESC LIMIT 1",
    "type": "ranking"
  }},
  {{
    "natural_language": "How many players are forwards?",
    "sql": "SELECT COUNT(*) as forward_count FROM common_player_info WHERE position LIKE '%F%' LIMIT 1",
    "type": "counting"
  }},
  {{
    "natural_language": "List all games where both teams scored over 100 points.",
    "sql": "SELECT g.game_id FROM game g WHERE g.pts_home > 100 AND g.pts_away > 100",
    "type": "filtering"
  }},
  {{
    "natural_language": "What's the most common height among NBA players?",
    "sql": "SELECT height FROM common_player_info WHERE height != '' GROUP BY height ORDER BY COUNT(*) DESC LIMIT 1",
    "type": "aggregation"
  }}
]
</example>"""

    # Add feedback section if expected SQL is provided
    if feedback_mode:
        prompt_text += """
Now, based on your analysis, please provide the SQL query that would answer this natural language question. Write your SQL query inside <sql_query> tags.

Now, let's compare your SQL query to the expected SQL query:

Expected SQL:
<expected_sql>
{1}
</expected_sql>

When you reply, first plan on how you should answer within <thinking> </thinking>. This is a place to write down relevant content and will not be shown to the user. 

Once you are done thinking, output your final answer to the user within <answer> </answer>. Make sure your answer is formatted exactly as described. If the queries do not match, please provide feedback within <feedback></feedback> tags if the queries are similar, would return the same result, and have similar efficiency. If the queries match identically, then no feedback is necessary, but please still output the response that we have an exact match.
"""
    else:
        prompt_text += """
Now, based on your analysis, please provide the SQL query that would answer this natural language question. Write your SQL query inside <sql_query> tags.

When you reply, first plan on how you should answer within <thinking> </thinking>. This is a place to write down relevant content and will not be shown to the user. 

Once you are done thinking, output your final answer to the user within <answer> </answer>. Make sure your answer is formatted exactly as described.
"""

    # Format the prompt
    if feedback_mode:
        formatted_prompt = prompt_text.format(question, expected_sql)
    else:
        formatted_prompt = prompt_text.format(question)
    
    try:
        response = client.messages.create(
            model="claude-3-opus-20240229",
            max_tokens=1000,
            messages=[{"role": "user", "content": formatted_prompt}]
        )
        
        # Extract Claude's response
        answer = response.content[0].text
        
        # Extract SQL and feedback sections
        sql_match = re.search(r'<sql_query>(.*?)</sql_query>', answer, re.DOTALL)
        feedback_match = re.search(r'<feedback>(.*?)</feedback>', answer, re.DOTALL)
        
        sql = sql_match.group(1).strip() if sql_match else ""
        feedback = feedback_match.group(1).strip() if feedback_match else ""
        
        return sql, feedback
    
    except Exception as e:
        print(f"❌ Error calling Claude API: {e}")
        return None, None

# --- Testing Functions ---

def test_single_query(conn, ground_truth_item, use_feedback=False, verbose=True):
    """
    Test a single natural language query against the ground truth.
    Returns a dictionary with test results.
    """
    question = ground_truth_item["natural_language"]
    ground_truth_sql = ground_truth_item["sql"]
    
    if verbose:
        print(f"🔍 Testing: {question}")
    
    # Get SQL from Claude - with or without feedback
    if use_feedback:
        claude_sql, feedback = get_sql_from_claude_with_feedback(question, ground_truth_sql)
    else:
        claude_sql, _ = get_sql_from_claude_with_feedback(question)
    
    if claude_sql is None:
        return {
            "question": question,
            "ground_truth_sql": ground_truth_sql,
            "claude_sql": None,
            "success": False,
            "error": "Failed to get SQL from Claude"
        }
    
    # Execute both queries
    ground_truth_results = execute_sql_query(conn, ground_truth_sql)
    claude_results = execute_sql_query(conn, claude_sql)
    
    # Compare results
    success = compare_query_results(ground_truth_results, claude_results)
    
    if verbose:
        print(f"Ground Truth SQL: {ground_truth_sql}")
        print(f"Claude's SQL: {claude_sql}")
        print(f"Success: {'✅' if success else '❌'}")
        
        if use_feedback and feedback:
            print(f"Feedback: {feedback}")
            
        if not success and ground_truth_results is not None and claude_results is not None:
            print("\nGround Truth Results:")
            print(f"Columns: {ground_truth_results['column_names']}")
            for i, row in enumerate(ground_truth_results['rows'][:5]):  # Show up to 5 rows
                print(f"Row {i+1}: {row}")
                
            print("\nClaude Results:")
            print(f"Columns: {claude_results['column_names']}")
            for i, row in enumerate(claude_results['rows'][:5]):  # Show up to 5 rows
                print(f"Row {i+1}: {row}")
        
        print("-" * 50)
    
    return {
        "question": question,
        "ground_truth_sql": ground_truth_sql,
        "claude_sql": claude_sql,
        "success": success,
        "feedback": feedback if feedback and use_feedback else None
    }

def run_example_tests(conn, ground_truth_data, num_examples=5, use_feedback=False):
    """Run tests on a random sample of examples from ground truth data."""
    if len(ground_truth_data) == 0:
        print("❌ No ground truth data available.")
        return
    
    # Select random examples
    examples = random.sample(ground_truth_data, min(num_examples, len(ground_truth_data)))
    
    results = []
    for example in examples:
        result = test_single_query(conn, example, use_feedback=use_feedback)
        results.append(result)
    
    # Display summary
    successes = sum(1 for r in results if r["success"])
    print(f"\n📊 Summary: {successes}/{len(results)} tests passed ({successes/len(results)*100:.1f}%)")
    
    return results

# --- Main Execution ---

# Load ground truth data
ground_truth_data = load_ground_truth_data()

# Connect to database
conn = connect_to_db()

if conn and ground_truth_data:
    # Run tests on 5 random examples with feedback
    print("\n=== Testing with feedback mechanism ===")
    feedback_results = run_example_tests(conn, ground_truth_data, num_examples=5, use_feedback=True)
else:
    print("❌ Cannot proceed with testing due to setup errors.")

# Close the database connection
if conn:
    conn.close()