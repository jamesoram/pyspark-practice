from io import StringIO
import csv

words = ["employee", "salary", "csv"]
file_path = "employees.csv"

try:
    with open(file_path, "r", encoding="utf-8") as f:
        csv_reader = csv.reader(f)
        tokens = list(csv_reader)

        words_in_file = [word for row in tokens for word in row]
        print(words_in_file)
        found = []
        for word in words:
            if word in words_in_file:
                found.append(word)

    if found:
        print(f"found tokens: {found}")
    else:
        print("nothing found")

except:
    print("Error opening file " + file_path)
