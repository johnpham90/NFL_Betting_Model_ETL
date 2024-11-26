from prefect import flow, task
import os
import pandas as pd

@task
def load_excel_files(base_path, year, week):
    """
    Load all Excel files from the specified directory into individual DataFrames.

    Args:
        base_path (str): The base directory where all data is stored.
        year (str): The specific year folder (e.g., "2023").
        week (str): The specific week folder (e.g., "Week_1").

    Returns:
        dict: A dictionary where keys are file names (without extensions) and values are DataFrames.
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
    Populate 'awayteamid' and 'hometeamid' columns based on the team name.

    Args:
        dataframes (dict): A dictionary where keys are file names and values are DataFrames.

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
        "Kansas City Chiefs": "KC",
        "Los Angeles Chargers": "LAC",
        "San Diego Chargers": "LAC",
        "Los Angeles Rams": "LAR",
        "St. Louis Rams": "LAR",
        "Miami Dolphins": "MIA",
        "Minnesota Vikings": "MIN",
        "New Orleans Saints": "NO",
        "New York Giants": "NYG",
        "New York Jets": "NYJ",
        "Philadelphia Eagles": "PHI",
        "Pittsburgh Steelers": "PIT",
        "San Francisco 49ers": "SF",
        "Seattle Seahawks": "SEA",
        "St. Louis Cardinals": "ARI",
        "Tennessee Titans": "TEN",
        "Washington Commanders": "WAS",
        "Washington Football Team": "WAS",
        "Washington Redskins": "WAS",
        "Baltimore Colts": "IND",
        "Indianapolis Colts": "IND",
        "Las Vegas Raiders": "LVR",
        "Oakland Raiders": "LVD",
        "Green Bay Packers": "GNB",
        "Tampa Bay Buccaneers": "TAM",
        "New England Patriots": "NWE"
    }

    for file_name, df in dataframes.items():
        try:
            df['awayteamid'] = df['Away Team'].map(team_mapping)  # Replace with actual column name
            df['hometeamid'] = df['Home Team'].map(team_mapping)  # Replace with actual column name
            print(f"Populated team IDs for: {file_name}")
        except KeyError as e:
            print(f"Missing expected column in {file_name}: {e}")

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
            required_columns = ['Season', 'Week', 'awayteamid', 'hometeamid']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                print(f"Missing required columns in {file_name}: {missing_columns}")
                continue

            # Create the gamesummaryid
            df['gamesummaryid'] = (
                "gs-" +
                df['Season'].astype(str) +
                df['Week'].astype(str).str.zfill(2) +  # Ensure Week is zero-padded (e.g., Week 1 -> '01')
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
                required_columns = ['Season', 'Week', 'awayteamid', 'hometeamid', 'quarter']
                if not all(col in df.columns for col in required_columns):
                    print(f"Missing required columns in {key}: {required_columns}")
                    continue

                # Create drivestatsid
                df['drivestatsid'] = (
                    "ds-" +
                    df['Season'].astype(str) +
                    df['Week'].astype(str).str.zfill(2) +
                    df['awayteamid'].astype(str) +
                    df['hometeamid'].astype(str) +
                    df['quarter'].astype(str)
                )

                print(f"Assigned 'drivestatsid' for: {key}")
                # Print sample results
                print(f"Sample results for '{key}' (Drive Stats):")
                print(df[['Season', 'Week', 'awayteamid', 'hometeamid', 'quarter', 'drivestatsid']].head())
            except Exception as e:
                print(f"Error processing {key}: {e}")

        # Process Drive Details DataFrame
        elif key.endswith("Drive_Details"):
            try:
                required_columns = ['Season', 'Week', 'awayteamid', 'hometeamid', 'Quarter']
                if not all(col in df.columns for col in required_columns):
                    print(f"Missing required columns in {key}: {required_columns}")
                    continue

                # Create drivestatsid
                df['drivestatsid'] = (
                    "ds-" +
                    df['Season'].astype(str) +
                    df['Week'].astype(str).str.zfill(2) +
                    df['awayteamid'].astype(str) +
                    df['hometeamid'].astype(str) +
                    df['Quarter'].astype(str)
                )

                print(f"Assigned 'drivestatsid' for: {key}")
                # Print sample results
                print(f"Sample results for '{key}' (Drive Details):")
                print(df[['Season', 'Week', 'awayteamid', 'hometeamid', 'Quarter', 'drivestatsid']].head())
            except Exception as e:
                print(f"Error processing {key}: {e}")

    return dataframes

@flow
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

if __name__ == "__main__":
    # Example inputs
    base_directory = r"C:\NFLStats\data"
    year_input = "2023"
    week_input = "Week_1"

    # Run the flow
    transform_stats(base_path=base_directory, year=year_input, week=week_input)