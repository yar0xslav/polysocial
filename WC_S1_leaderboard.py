import psycopg2
import time

# Connect to the db
conn = psycopg2.connect(
    database="hub", user="app", password="password", host="localhost", port="6541"
)
cur = conn.cursor()


# Query data
def execute_query(query):
    cur.execute(query)
    return cur.fetchall()


# Update the leaderboard
def update_leaderboard():
    # Define playing users
    playing_users_query = "SELECT value, fid, start_date FROM user_data WHERE type = 6 AND start_date IS NOT NULL"
    playing_users = execute_query(playing_users_query)

    print("Playing users:", playing_users)  # Print playing users

    # Clear leaderboard table
    clear_query = "TRUNCATE TABLE leaderboard"
    cur.execute(clear_query)
    conn.commit()
    print("Leaderboard table cleared")

    for user in playing_users:
        # Determine target_hash for user's bets
        bets_query = f"""
            SELECT timestamp, 
                encode(target_hash, 'hex') AS target_hash_hex
            FROM (
                SELECT 
                    timestamp, 
                    target_hash, 
                    ROW_NUMBER() OVER (PARTITION BY timestamp::date ORDER BY timestamp ASC) AS row_num
                FROM reactions
                WHERE fid = {user[1]}
                    AND reaction_type = 1
                    AND timestamp::date >= '{user[2]}'
            ) ranked_reactions
            WHERE row_num <= 5
            ORDER BY timestamp ASC
        """
        bets = execute_query(bets_query)

        total_points_earned = 0
        total_bets_made = len(bets)

        for bet in bets:
            timestamp, target_hash_hex = bet
            target_hash = "\\x" + target_hash_hex

            points_query = f"""
                SELECT total_count - row_number AS result
                FROM (
                    SELECT COUNT(*) AS total_count
                    FROM reactions
                    WHERE reaction_type = 1 AND target_hash = '{target_hash}'
                ) total_count_subquery,
                (
                    SELECT COUNT(*) AS row_number
                    FROM reactions
                    WHERE reaction_type = 1 AND target_hash = '{target_hash}'
                    AND timestamp < (
                        SELECT MAX (timestamp)
                        FROM reactions
                        WHERE reaction_type = 1 AND target_hash = '{target_hash}' AND fid = {user[1]}
                    )
                ) row_number_subquery;
            """
            print("Points query:", points_query)  # Print points query
            points_result = execute_query(points_query)
            print("Points result:", points_result)  # Print points result

            points_earned = points_result[0][0]
            print(f"Points earned: {points_earned}")
            total_points_earned += points_earned

            print(
                "Total points earned:", total_points_earned
            )  # Print total points earned

        # Calculate points_per_bet
        if total_bets_made > 0:
            points_per_bet = total_points_earned / total_bets_made
        else:
            points_per_bet = 0

        # Insert into leaderboard table
        insert_query = f"INSERT INTO leaderboard (username, total_points_earned, total_bets_made, points_per_bet) VALUES ('{user[0]}', {total_points_earned}, {total_bets_made}, {points_per_bet})"
        cur.execute(insert_query)
        conn.commit()

    print("Leaderboard has been updated!")


# Add new player and set a start date
def add_player(fid, start_date):
    # Check if the player already exists in the user_data table
    check_query = f"SELECT value FROM user_data WHERE fid = {fid}"
    result = execute_query(check_query)

    if result:
        # Check if the user has already joined the game
        start_date_query = f"SELECT start_date FROM user_data WHERE fid = {fid} AND start_date IS NOT NULL"
        joined = execute_query(start_date_query)

        if joined:
            print(f"Player with fid {fid} has already joined the game.")
        else:
            # Update the start date for the existing player
            update_start_date_query = (
                f"UPDATE user_data SET start_date = '{start_date}' WHERE fid = {fid}"
            )
            cur.execute(update_start_date_query)
            conn.commit()
            print(f"Updated start date for player with fid {fid} to {start_date}")
    else:
        print(f"Player with fid {fid} does not exist in the user_data table.")


# Start script and update leaderboard every 15 mins
def main():
    while True:
        update_leaderboard()
        time.sleep(900)  # 900 secs = 15 mins


# Start main function
if __name__ == "__main__":
    main()
