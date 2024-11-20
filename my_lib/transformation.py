def transform(conn):
    query = '''
    CREATE TABLE serious_injury_outcome_indicators_transformed AS
    SELECT 
        *,
        EXTRACT(YEAR FROM Period) AS year,
        CASE 
            WHEN EXTRACT(YEAR FROM Period) < 2010 THEN 1 
            ELSE 0 
        END AS before_2010
    FROM serious_injury_outcome_indicators_2000_2022;
    '''

    cursor = conn.cursor()

    cursor.execute('''USE my_database;''')
    cursor.execute(query)
    results = cursor.fetchall()
    for row in results:
        print(row)

    return results