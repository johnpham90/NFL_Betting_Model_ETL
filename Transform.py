from prefect import flow, task
import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from io import StringIO
from typing import List


@task
@task
def load_excel_files(base_path, year, week):
    """
    Load all Excel files from the specified directory into individual DataFrames,
    standardizing column names to lowercase and removing spaces for database compatibility.
    """
    target_directory = os.path.join(base_path, year, week)
    dataframes = {}

    if not os.path.exists(target_directory):
        print(f"Directory not found: {target_directory}")
        return dataframes

    for file_name in os.listdir(target_directory):
        if file_name.endswith(".xlsx") or file_name.endswith(".xls"):
            file_path = os.path.join(target_directory, file_name)
            try:
                df = pd.read_excel(file_path)

                # Standardize column names: lowercase and remove spaces
                df.columns = df.columns.str.lower().str.replace(' ', '').str.rstrip('.')

                key_name = os.path.splitext(file_name)[0]
                dataframes[key_name] = df
                print(f"Loaded: {file_name} into DataFrame '{key_name}'")
            except Exception as e:
                print(f"Error loading {file_name}: {e}")

    return dataframes


# Step 2: Populate Team IDs
@task
def populate_team_ids(dataframes):
    """
    Populate 'teamid', 'awayteamid' and 'hometeamid' columns based on the team name.
    Also handles special cases for drivestats and teamstats tables.

    Returns:
        dict: A dictionary with updated DataFrames.
    """
    # Team name to ID mapping
    team_mapping = {
        "Arizona Cardinals": "ARI",
        "Phoenix Cardinals": "ARI",
        "Atlanta Falcons": "ATL",
        "Baltimore Ravens": "BAL",
        "Buffalo Bills": "BUF",
        "Carolina Panthers": "CAR",
        "Chicago Bears": "CHI",
        "Cincinnati Bengals": "CIN",
        "Cleveland Browns": "CLE",
        "Dallas Cowboys": "DAL",
        "Denver Broncos": "DEN",
        "Detroit Lions": "DET",
        "Houston Texans": "HOU",
        "Houston Oilers": "HOU",
        "Jacksonville Jaguars": "JAX",
        "Kansas City Chiefs": "KAN",
        "Los Angeles Chargers": "LAC",
        "San Diego Chargers": "LAC",
        "Los Angeles Rams": "LAR",
        "St. Louis Rams": "LAR",
        "Miami Dolphins": "MIA",
        "Minnesota Vikings": "MIN",
        "New Orleans Saints": "NOR",
        "New York Giants": "NYG",
        "New York Jets": "NYJ",
        "Philadelphia Eagles": "PHI",
        "Pittsburgh Steelers": "PIT",
        "San Francisco 49ers": "SFO",
        "Seattle Seahawks": "SEA",
        "St. Louis Cardinals": "ARI",
        "Tennessee Titans": "TEN",
        "Washington Commanders": "WAS",
        "Washington Football Team": "WAS",
        "Washington Redskins": "WAS",
        "Baltimore Colts": "IND",
        "Indianapolis Colts": "IND",
        "Las Vegas Raiders": "LVR",
        "Oakland Raiders": "LVR",
        "Green Bay Packers": "GNB",
        "Tampa Bay Buccaneers": "TAM",
        "New England Patriots": "NWE"
    }

    team_rename_patterns = [
        'Defense_Stats',
        'Drives_Details',
        'Kicking_Stats',
        'Passing_Stats',
        'Player_Offense_Stats',
        'Player_Defense_Stats',
        'Receiving_Stats',
        'Returns_Stats',
        'Rushing_Stats'
    ]

    for file_name, df in dataframes.items():
        try:
            # Handle awayteamid and hometeamid for all tables
            if 'awayteam' in df.columns:
                df['awayteamid'] = df['awayteam'].map(team_mapping)
            if 'hometeam' in df.columns:
                df['hometeamid'] = df['hometeam'].map(team_mapping)

            # Handle specific case for defense table
            if 'Player_Defense_Stats' in file_name:
                if 'team' in df.columns:
                    # Keep original team column and create teamid
                    df['teamid'] = df['team']
                    print(f"Created teamid from team in {file_name}")
            # Handle other pattern matches
            elif any(pattern in file_name for pattern in team_rename_patterns):
                if 'team' in df.columns:
                    df.rename(columns={'team': 'teamid'}, inplace=True)
                    print(f"Renamed 'team' to 'teamid' in {file_name}")

            
            """Handle team -> teamid mapping
            if 'team' in df.columns:
                df['teamid'] = df['team'].map(team_mapping)
                df.drop('team', axis=1, inplace=True)
            """

            # Special handling for drivestats and teamstats
            if 'Drives_Stats' in file_name or 'Team_Stats_Stats' in file_name:
                # If 'team' column exists, map it to teamid
                if 'team' in df.columns:
                    df['teamid'] = df['team'].map(team_mapping)
                    df.drop('team', axis=1, inplace=True)
                # If team name is in a different column, handle that case
                elif 'Team' in df.columns:
                    df['teamid'] = df['Team'].map(team_mapping)
                    df.drop('Team', axis=1, inplace=True)
                
            print(f"Populated team IDs for: {file_name}")
            
            # Debug output to verify
            if 'Drives_Stats' in file_name or 'Team_Stats_Stats' in file_name:
                if 'teamid' in df.columns:
                    print(f"Sample teamids for {file_name}:")
                    print(df['teamid'].head())
                else:
                    print(f"Warning: No teamid column created for {file_name}")
                    
        except KeyError as e:
            print(f"Missing expected column in {file_name}: {e}")
        except Exception as e:
            print(f"Error processing {file_name}: {str(e)}")


    return dataframes

# Step 3: Assign Game Summary IDs
@task
def assign_gamesummary_ids(dataframes):
    """
    Assign 'gamesummaryid' based on concatenation of season, week, awayteamid, and hometeamid.

    Args:
        dataframes (dict): A dictionary where keys are file names and values are DataFrames.

    Returns:
        dict: A dictionary with updated DataFrames.
    """
    for file_name, df in dataframes.items():
        try:
            # Ensure the required columns exist
            required_columns = ['season', 'week', 'awayteamid', 'hometeamid']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                print(f"Missing required columns in {file_name}: {missing_columns}")
                continue

            # Create the gamesummaryid
            df['gamesummaryid'] = (
                "gs-" +
                df['season'].astype(str) +
                df['week'].astype(str).str.zfill(2) +  # Ensure Week is zero-padded (e.g., Week 1 -> '01')
                df['awayteamid'].astype(str) +
                df['hometeamid'].astype(str)
            )

            print(f"Assigned 'gamesummaryid' for: {file_name}")
        except Exception as e:
            print(f"Error assigning 'gamesummaryid' in {file_name}: {e}")

    return dataframes

# Step 4: Assign Drive Stats IDs
@task
def assign_drivestats_ids(dataframes):
    """
    Assign 'drivestatsid' to Drive Stats and Drive Details DataFrames.

    Args:
        dataframes (dict): A dictionary of DataFrames loaded from Excel files.

    Returns:
        dict: Updated dictionary of DataFrames with 'drivestatsid' added to the relevant DataFrames.
    """
    for key, df in dataframes.items():
        # Process Drive Stats DataFrame
        if key.endswith("Drive_Stats"):
            try:
                required_columns = ['season', 'week', 'awayteamid', 'hometeamid', 'quarter']
                if not all(col in df.columns for col in required_columns):
                    print(f"Missing required columns in {key}: {required_columns}")
                    continue

                # Create drivestatsid
                df['drivestatsid'] = (
                    "ds-" +
                    df['season'].astype(str) +
                    df['week'].astype(str).str.zfill(2) +
                    df['awayteamid'].astype(str) +
                    df['hometeamid'].astype(str) +
                    df['quarter'].astype(str)
                )

                print(f"Assigned 'drivestatsid' for: {key}")
                # Print sample results
                print(f"Sample results for '{key}' (Drive Stats):")
                print(df[['season', 'week', 'awayteamid', 'hometeamid', 'quarter', 'drivestatsid']].head())
            except Exception as e:
                print(f"Error processing {key}: {e}")

        # Process Drive Details DataFrame
        elif key.endswith("Drive_Details"):
            try:
                required_columns = ['season', 'week', 'awayteamid', 'hometeamid', 'Quarter']
                if not all(col in df.columns for col in required_columns):
                    print(f"Missing required columns in {key}: {required_columns}")
                    continue

                # Create drivestatsid
                df['drivestatsid'] = (
                    "ds-" +
                    df['season'].astype(str) +
                    df['week'].astype(str).str.zfill(2) +
                    df['awayteamid'].astype(str) +
                    df['hometeamid'].astype(str) +
                    df['Quarter'].astype(str)
                )

                print(f"Assigned 'drivestatsid' for: {key}")
                # Print sample results
                print(f"Sample results for '{key}' (Drive Details):")
                print(df[['season', 'week', 'awayteamid', 'hometeamid', 'Quarter', 'drivestatsid']].head())
            except Exception as e:
                print(f"Error processing {key}: {e}")

    return dataframes

# Database connection parameters
DB_PARAMS = {
    "host": "localhost",
    "database": "NFL_Betting_Model",
    "user": "postgres",
    "password": "Football1!"
}

def add_missing_columns(cursor, table_name, df):
    """
    Add any missing columns to the specified table.
    """
    cursor.execute(f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = 'stats' 
        AND table_name = %s
    """, (table_name,))
    existing_columns = {row[0] for row in cursor.fetchall()}

    missing_columns = set(df.columns) - existing_columns
    if missing_columns:
        print(f"Adding missing columns to {table_name}: {list(missing_columns)}")
        for col in missing_columns:
            sql_type = get_sql_type(df[col].dtype)
            alter_table_sql = f'ALTER TABLE stats."{table_name}" ADD COLUMN "{col}" {sql_type}'
            cursor.execute(alter_table_sql)

def load_table(cursor, table_name, df):
    """
    Load data into a table using a temporary table for efficiency.
    """
    # Create temp table without schema qualification in the name
    temp_table = f'temp_{table_name}'
    
    # Drop temp table if it exists
    cursor.execute(f'DROP TABLE IF EXISTS "{temp_table}"')
    
    # Create temp table copying structure from the target table
    cursor.execute(f"""
        CREATE TABLE "{temp_table}" 
        (LIKE stats."{table_name}" INCLUDING ALL)
    """)

    # Convert float columns to appropriate format
    df_copy = df.copy()
    for col in df_copy.columns:
        if df_copy[col].dtype == 'float64':
            if col in ['def_int']:  # Add other integer columns here if needed
                df_copy[col] = df_copy[col].fillna(0).astype(int)

    # Copy data to temp table
    output = StringIO()
    df_copy.to_csv(output, sep='\t', header=False, index=False, na_rep='\\N')
    output.seek(0)
    
    # For copy_from, use the table name without schema
    cursor.copy_from(
        file=output,
        table=temp_table,
        null='\\N',
        columns=df_copy.columns
    )

    # Insert data from temp to target table
    columns_quoted = [f'"{col}"' for col in df_copy.columns]
    columns_str = ', '.join(columns_quoted)
    
    cursor.execute(f"""
        INSERT INTO stats."{table_name}" ({columns_str})
        SELECT {columns_str}
        FROM "{temp_table}"
    """)

    # Clean up temp table
    cursor.execute(f'DROP TABLE IF EXISTS "{temp_table}"')

def load_to_postgres(dataframes, year, week):
    """
    Load dataframes into PostgreSQL, handling both data type conversions and loading order.
    """
    # Define integer columns for ALL tables that need integer conversion
    integer_columns = {
        'defense_advanced': ['def_int', 'sacks', 'blitzes', 'qb_hurry', 'qb_knockdown', 
                           'pressures', 'tackles_combined', 'tackles_missed'],
        'defense': ['def_int', 'tackles_solo', 'tackles_assists', 'tackles_combined', 
                   'tackles_loss', 'sacks', 'qb_hits', 'fumbles_forced', 'fumbles_rec',
                   'fumbles_rec_yds', 'fumbles_rec_td', 'def_int_yds', 'def_int_td',
                   'pass_defended']
    }
    
    # Define table mapping
    table_mapping = {
        'Defense_Stats': 'defense_advanced',     
        'Player_Defense_Stats': 'defense',      
        'Drives_Stats': 'drivestats',
        'Drive_Details': 'drivedetails',
        'Game_Summary_Stats': 'gamesummary',
        'Kicking_Stats': 'kicking',
        'Passing_Stats': 'passing',
        'Player_Offense_Stats': 'offense',
        'Receiving_Stats': 'receiving',
        'Returns_Stats': 'returns',
        'Rushing_Stats': 'rushing',
        'Team_Stats_Stats': 'teamstats'
    }
    
    # Define strict loading order
    loading_order = [
        'Game_Summary_Stats',      # Must be first
        'Player_Defense_Stats',    
        'Defense_Stats',
        'Drives_Stats',
        'Drive_Details',
        'Kicking_Stats',
        'Passing_Stats',
        'Player_Offense_Stats',
        'Receiving_Stats',
        'Returns_Stats',
        'Rushing_Stats',
        'Team_Stats_Stats'
    ]
    
    try:
        with psycopg2.connect(**DB_PARAMS) as conn:
            conn.autocommit = False
            with conn.cursor() as cursor:
                cursor.execute("SET search_path TO stats;")
                
                print("\nStarting data load process...")
                print("Available dataframes:", list(dataframes.keys()))
                
                # First, ensure Game_Summary_Stats exists and process it first
                game_summary_found = False
                for df_name in dataframes.keys():
                    if 'Game_Summary_Stats' in df_name:
                        game_summary_found = True
                        break
                
                if not game_summary_found:
                    print("ERROR: Game Summary Stats data is required but not found!")
                    return
                
                # Process dataframes in strict order
                for table_type in loading_order:
                    # Find the matching dataframe
                    df_name = None
                    for name in dataframes.keys():
                        if table_type in name:
                            df_name = name
                            break
                    
                    if not df_name:
                        print(f"Skipping {table_type} - no matching dataframe found")
                        continue
                        
                    df = dataframes[df_name]
                    target_table = table_mapping[table_type]
                    print(f"\nProcessing {df_name} -> {target_table}")
                    
                    # Make a copy of the dataframe
                    df_copy = df.copy()
                    
                    # Convert integer columns if needed
                    if target_table in integer_columns:
                        print(f"Converting integer columns for {target_table}")
                        for col in integer_columns[target_table]:
                            if col in df_copy.columns:
                                print(f"  Converting {col} to integer")
                                df_copy[col] = df_copy[col].fillna(0).astype(int)
                    
                    # Check and add missing columns
                    cursor.execute("""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_schema = 'stats' 
                        AND table_name = %s
                    """, (target_table,))
                    existing_columns = {row[0] for row in cursor.fetchall()}
                    
                    missing_columns = set(df_copy.columns) - existing_columns
                    if missing_columns:
                        print(f"Adding missing columns to {target_table}: {list(missing_columns)}")
                        for col in missing_columns:
                            sql_type = get_sql_type(df_copy[col].dtype)
                            alter_table_sql = f'ALTER TABLE stats."{target_table}" ADD COLUMN "{col}" {sql_type}'
                            cursor.execute(alter_table_sql)
                    
                    # Create temp table
                    temp_table = f'temp_{target_table}'
                    cursor.execute(f'DROP TABLE IF EXISTS "{temp_table}"')
                    cursor.execute(f"""
                        CREATE TABLE "{temp_table}" 
                        (LIKE stats."{target_table}" INCLUDING ALL)
                    """)
                    
                    # Load data to temp table
                    output = StringIO()
                    df_copy.to_csv(output, sep='\t', header=False, index=False, na_rep='\\N')
                    output.seek(0)
                    
                    try:
                        cursor.copy_from(
                            file=output,
                            table=temp_table,
                            null='\\N',
                            columns=df_copy.columns
                        )
                    except Exception as e:
                        print(f"Error during data copy for {target_table}:")
                        print(f"Columns in dataframe: {list(df_copy.columns)}")
                        print(f"First few rows of data:")
                        print(df_copy.head())
                        raise
                    
                    # Insert from temp to target table
                    columns_quoted = [f'"{col}"' for col in df_copy.columns]
                    columns_str = ', '.join(columns_quoted)
                    cursor.execute(f"""
                        INSERT INTO stats."{target_table}" ({columns_str})
                        SELECT {columns_str}
                        FROM "{temp_table}"
                    """)
                    
                    # Clean up
                    cursor.execute(f'DROP TABLE IF EXISTS "{temp_table}"')
                    
                    # Commit after each successful table load
                    conn.commit()
                    print(f"Successfully loaded {target_table}")
                
                print(f"\nSuccessfully loaded all data for {year} week {week}")
                
    except Exception as e:
        print(f"\nError loading data to PostgreSQL: {str(e)}")
        if 'conn' in locals():
            conn.rollback()
        raise

def get_sql_type(dtype):
    """
    Map pandas dtypes to PostgreSQL data types
    """
    if pd.api.types.is_integer_dtype(dtype):
        return 'INTEGER'
    elif pd.api.types.is_float_dtype(dtype):
        return 'DOUBLE PRECISION'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'TIMESTAMP'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    else:
        return 'TEXT'

# Data loading flow
@flow(name="Load Stats to PostgreSQL")
def load_stats_to_postgres(dataframes, year, week):
    load_to_postgres(dataframes, year, week)

@flow(name="Transform NFL Stats")
def transform_stats(base_path: str, year: str, week: str):
    """
    A Prefect flow that loads and transforms data.

    Args:
        base_path (str): The base directory where all data is stored.
        year (str): The specific year folder (e.g., "2023").
        week (str): The specific week folder (e.g., "Week_1").
    """
    # Step 1: Load Excel files
    dataframes = load_excel_files(base_path, year, week)

    # Step 2: Populate team IDs
    dataframes = populate_team_ids(dataframes)

    # Step 3: Assign game summary IDs
    dataframes = assign_gamesummary_ids(dataframes)

    # Step 4: Assign drive stats IDs
    dataframes = assign_drivestats_ids(dataframes)

    # Print transformed DataFrame keys for verification
    for key, df in dataframes.items():
        print(f"Final Transformed DataFrame '{key}' - Preview:")
        print(df.head())

    return dataframes

@flow(name="NFL Stats Master Pipeline")
def process_multiple_periods(
    base_directory: str,
    years: List[str],
    weeks: List[int]
) -> None:
    """
    Master flow that processes multiple periods while maintaining data integrity
    and sequential processing.
    """
    for year in years:
        for week in weeks:
            try:
                # Format week string
                week_str = f"Week_{week}"
                print(f"\n{'='*50}")
                print(f"Starting process for Year: {year}, Week: {week_str}")
                print(f"{'='*50}")

                # Step 1: Transform Flow - includes all sequential transformations
                transformed_data = transform_stats(
                    base_path=base_directory,
                    year=year,
                    week=week_str
                )

                if transformed_data:
                    # Step 2: Database Load Flow - only if transformation succeeds
                    load_stats_to_postgres(
                        dataframes=transformed_data,
                        year=year,
                        week=week
                    )
                    print(f"\nSuccessfully completed full process for Year {year} Week {week_str}")
                else:
                    print(f"\nSkipping database load for Year {year} Week {week_str} - transformation failed")

            except Exception as e:
                print(f"\nError processing Year {year} Week {week_str}: {str(e)}")
                continue

if __name__ == "__main__":
    # Configuration
    base_directory = r"C:\NFLStats\data"
    years_to_process = ["2023"]  # Add more years as needed
    weeks_to_process = list(range(1, 2))  # Weeks 1-22

    # Run the master flow
    process_multiple_periods(
        base_directory=base_directory,
        years=years_to_process,
        weeks=weeks_to_process
    )