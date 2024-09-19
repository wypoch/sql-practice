import sqlite3 as sql
import csv
import glob
from matplotlib import pyplot as plt

def get_csv_data(csv_file):
    """Return a list of tuples representing each row of the csv (except the first row)"""
    with open(csv_file,'r') as infile:
        rows = list(csv.reader(infile))
    return rows[1:]

def populate_db(dir=None):
    """Populates the database with the .csv files in the working directory.
    Assumes that the files are located in 'dir', if provided.
    There should be a distinct .csv for each semester.
    Additionally, each file should be titled SEM.csv where SEM is the semester.

    The schema of each .csv are as follows (in order):
    Course ID (string): a string uniquely identifying the course
    Size (int): the enrollment cap for the course
    NumEnrl (int): the number of students who actuall enrolled
    Name (string): the name of the course
    """

    # Get .csv files in current directory
    csv_files = glob.glob('*.csv',root_dir=dir)
    table_names = []

    # Populate the database with tables for each semester
    # containing information about each course offered
    for file in csv_files:
        sem_data = get_csv_data(file)
        # Name each table after the current semester (strip ".csv")
        table_name = file.strip(".csv")
        table_names.append(table_name)
        try:
            with sql.connect("classes.db") as conn:
                # Insert the data from the csv into each corresponding table
                cur = conn.cursor()
                cur.execute(f"DROP TABLE IF EXISTS {table_name};")
                cur.execute(f"CREATE TABLE {table_name}(ID TEXT, Size INT, NumEnrl INT, Name TEXT);")
                cur.executemany(f"INSERT INTO {table_name} VALUES(?,?,?,?);", sem_data)
        finally:
            conn.close()

    # Next, create a new table of aggregate data
    try:
        with sql.connect("classes.db") as conn:
            cur = conn.cursor()
            cur.execute("DROP TABLE IF EXISTS ClassData")
            cur.execute("CREATE TABLE ClassData(ID TEXT, Size INT, NumEnrl INT, Name TEXT, Semester TEXT)")

            # Populate the ClassData table by taking the union of the other tables
            first_table_name = table_names[0]
            QUERY = f"""INSERT INTO ClassData SELECT * FROM (SELECT *, '{first_table_name}' FROM {first_table_name}"""
            for name in table_names[1:]:
                QUERY += f"\nUNION SELECT *, '{name}' FROM {name}"
            QUERY += ");"
            cur.execute(QUERY)
    finally:
        conn.close()

def visualize_enrollment(threshold=1):
    """Contructs a bar graph of enrollments for the courses offered
    (where total enrollment is at least at the threshold), using the data
    contained in the ClassData table."""
    try:
        with sql.connect("classes.db") as conn:
            cur = conn.cursor()
            # Get tuples of the form (class name, class ID, total enrollment)
            stats = cur.execute("""SELECT ID, SUM(NumEnrl) AS TotalEnrl FROM ClassData
                                GROUP BY ID
                                ORDER BY TotalEnrl DESC""").fetchall()

            # For visualization purposes, exclude courses with lifetime enrollment less than the given threshold
            labels = [tup[0] for tup in stats if tup[1] >= threshold]
            enrollments = [tup[1] for tup in stats if tup[1] >= threshold]

            # Plot a horizontal bar graph giving the total enrollment for each course
            positions = range(len(labels))
            plt.barh(positions, enrollments, align="center")
            plt.yticks(positions, labels)
            plt.xlabel("Total Course Enrollment")
            plt.ylabel("Course ID")
            plt.show()

    finally:
        conn.close()

def visualize_popularity(threshold=1):
    """Contructs a bar graph of popularity of the courses offered
    (where total enrollment is at least at the threshold), using the data
    contained in the ClassData table.
    Popularity is defined as the ratio between total possible enrollment
    and actual enrollment."""
    try:
        with sql.connect("classes.db") as conn:
            cur = conn.cursor()
            # Get tuples of the form (class name, class ID, popularity)
            stats = cur.execute("""SELECT ID, CAST(SUM(NumEnrl) AS FLOAT)/CAST(SUM(Size) AS FLOAT)
                                AS Popularity, SUM(NumEnrl) AS TotalEnrl FROM ClassData
                                GROUP BY ID
                                ORDER BY Popularity DESC""").fetchall()

            # For visualization purposes, exclude courses with lifetime enrollment less than the given threshold
            labels = [tup[0] for tup in stats if tup[2] >= threshold]
            popularities = [tup[1] for tup in stats if tup[2] >= threshold]

            # Plot a horizontal bar graph giving the total enrollment for each course
            positions = range(len(labels))
            plt.barh(positions, popularities, align="center")
            plt.yticks(positions, labels)
            plt.xlabel("Course Popularity")
            plt.ylabel("Course ID")
            plt.show()

    finally:
        conn.close()

def search_db(descr):
    """Searches the database for courses corresponding to the description given.
    Will match the course ID, (partial) course name, or (partial) semester.

    RETURNS:
    (str) the search results
    """
    try:
        with sql.connect("classes.db") as conn:
            cur = conn.cursor()
            # Search for an exact match on the course ID
            # or a partial match on the course name or semester.
            # Order by course ID first and semester offered second.
            results = cur.execute("""SELECT ID, Name, Semester FROM ClassData
                                 WHERE ID LIKE ? OR Name LIKE '%' || ? || '%' OR Semester LIKE '%' || ? || '%'
                                 ORDER BY ID ASC, Semester DESC""",
                                  (descr,descr,descr,)).fetchall()

            # Format the search results
            answer = "Search Results:\n"
            answer += f"{'ID'}\t{'Name': <30}{'Semester': >10}\n"
            for tup in results:
                answer += f"{tup[0]}\t{tup[1]:<30}{tup[2]: >10}\n"
    finally:
        conn.close()

    return answer

if __name__ == '__main__':
    populate_db()
    visualize_enrollment(threshold=100)
    visualize_popularity(threshold=100)

    print(search_db('Algebra'))
    print(search_db('2020'))