def analyze(conn):
    """Perform a Spark SQL query."""
    query = """
    SELECT Severity, COUNT(*) AS total_cases
    FROM injuryOutcome
    GROUP BY Severity
    ORDER BY total_cases DESC
    """

    cursor = conn.cursor()

    cursor.execute('''USE my_database;''')
    cursor.execute(query)
    results = cursor.fetchall()
    for row in results:
        print(row)

    results.write.mode("overwrite").saveAsTable("my_database.injury_outcome_summary")